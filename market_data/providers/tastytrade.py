"""
market_data/providers/tastytrade.py
TastyTrade provider — OHLCV via DXLink WebSocket, options chain via REST.

Auth:  OAuth2 refresh-token flow (client_id + refresh_token → Bearer access token).
       Access tokens expire every 15 min; _ensure_token() refreshes transparently.

OHLCV: DXLink WebSocket streaming with Candle event subscriptions.
       Sandbox:    api.cert.tastyworks.com
       Production: api.tastyworks.com

Options chain: REST /option-chains/{symbol}/nested gives structure.
               REST /market-data/by-type gives bid/ask/last/volume/OI (batched ≤100).
               Greeks (delta/gamma/theta/vega/rho/IV) via DXLink Greeks events.

Config (.env):
    TASTYTRADE_CLIENT_ID=<oauth2 client id>
    TASTYTRADE_REFRESH_TOKEN=<long-lived refresh token>
    TASTYTRADE_SANDBOX=true        # false → production
"""

import asyncio
import json
import logging
import time
from datetime import date, datetime, timezone
from typing import Optional

import httpx
import pandas as pd

from market_data.config import settings
from market_data.models import DataType, Interval
from market_data.providers.base import BaseProvider, RateLimitConfig

logger = logging.getLogger(__name__)

SANDBOX_BASE = "https://api.cert.tastyworks.com"
PROD_BASE    = "https://api.tastyworks.com"

# DXLink candle interval tokens
INTERVAL_MAP: dict[Interval, str] = {
    Interval.ONE_MIN:     "1m",
    Interval.FIVE_MIN:    "5m",
    Interval.FIFTEEN_MIN: "15m",
    Interval.ONE_HOUR:    "1h",
    Interval.ONE_DAY:     "1d",
    Interval.ONE_WEEK:    "1w",
}

# Fields requested from DXLink for Candle events (COMPACT format)
CANDLE_FIELDS = ["eventSymbol", "time", "open", "high", "low", "close", "volume"]

# Fields requested from DXLink for Greeks events
GREEKS_FIELDS = [
    "eventSymbol", "volatility", "delta", "gamma", "theta", "vega", "rho", "price",
]

# Max symbols per /market-data/by-type request
MARKET_DATA_BATCH = 100

# Seconds to wait for new DXLink data before assuming stream is complete
DXLINK_IDLE_TIMEOUT = 8.0


class TastyTradeProvider(BaseProvider):
    """
    TastyTrade market data provider.

    Supports:
      - OHLCV (daily + intraday) via DXLink WebSocket
      - OPTIONS_CHAIN via REST + DXLink Greeks
    """

    name = "tastytrade"

    def __init__(self):
        self._client_id     = settings.tastytrade_client_id
        self._client_secret = settings.tastytrade_client_secret
        self._refresh_token = settings.tastytrade_refresh_token
        self._base_url      = SANDBOX_BASE if settings.tastytrade_sandbox else PROD_BASE
        self._access_token: Optional[str] = None
        self._token_expires_at: float = 0.0
        if not self._client_id or not self._refresh_token:
            logger.warning(
                "TASTYTRADE_CLIENT_ID or TASTYTRADE_REFRESH_TOKEN not set — "
                "provider will fail on requests"
            )
        super().__init__()

    # ── BaseProvider interface ─────────────────────────────────────────────

    def get_rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(
            calls_per_minute=60,
            min_interval_seconds=0.1,
        )

    def supported_data_types(self) -> list[DataType]:
        return [
            DataType.OHLCV,
            DataType.OHLCV_INTRADAY,
            DataType.OPTIONS_CHAIN,
        ]

    def _health_check(self) -> bool:
        try:
            self._ensure_token()
            data = self._get("/customers/me")
            return "data" in data
        except Exception as e:
            logger.warning(f"[tastytrade] health check failed: {e}")
            return False

    # ── OHLCV ─────────────────────────────────────────────────────────────

    def _fetch_ohlcv(
        self, symbol: str, start: date, end: date, interval: Interval
    ) -> pd.DataFrame:
        return asyncio.run(self._async_fetch_candles(symbol, start, end, interval))

    async def _async_fetch_candles(
        self, symbol: str, start: date, end: date, interval: Interval
    ) -> pd.DataFrame:
        # Acquire DXLink credentials
        token_resp = self._get("/api-quote-tokens")
        dxlink_url   = token_resp["data"]["dxlink-url"]
        dxlink_token = token_resp["data"]["token"]

        interval_str   = INTERVAL_MAP.get(interval, "1d")
        candle_symbol  = f"{symbol.upper()}{{={interval_str}}}"
        from_ms        = int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp() * 1000)
        end_ms         = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp() * 1000)

        logger.info(f"[tastytrade] DXLink candle fetch: {candle_symbol} from {start} to {end}")
        raw_candles = await _dxlink_fetch(
            url=dxlink_url,
            token=dxlink_token,
            subscriptions=[{"type": "Candle", "symbol": candle_symbol, "fromTime": from_ms}],
            event_fields={"Candle": CANDLE_FIELDS},
            target_event="Candle",
            idle_timeout=DXLINK_IDLE_TIMEOUT,
        )

        rows = []
        for c in raw_candles:
            t = c.get("time")
            if not t or t > end_ms:
                continue
            ts = datetime.fromtimestamp(t / 1000, tz=timezone.utc)
            if not (start <= ts.date() <= end):
                continue
            o, h, lo, cl = c.get("open"), c.get("high"), c.get("low"), c.get("close")
            # DXLink sends "NaN" (string) or float nan for non-trading periods
            def _is_nan(v) -> bool:
                if v is None:
                    return True
                try:
                    return float(v) != float(v)  # nan != nan
                except (TypeError, ValueError):
                    return True
            if any(_is_nan(v) for v in [o, h, lo, cl]):
                continue  # skip non-trading day candles
            def _vol(v) -> int:
                try:
                    f = float(v)
                    return 0 if f != f else int(f)
                except (TypeError, ValueError):
                    return 0
            rows.append({
                "timestamp": ts,
                "open":      float(o),
                "high":      float(h),
                "low":       float(lo),
                "close":     float(cl),
                "volume":    _vol(c.get("volume")),
            })

        if not rows:
            logger.warning(f"[tastytrade] No candle data returned for {symbol} {start}→{end}")
            return pd.DataFrame()

        df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
        return self._enforce_ohlcv_schema(df, symbol)

    # ── Options chain ──────────────────────────────────────────────────────

    def _fetch_options_chain(self, symbol: str, snapshot_date: Optional[date]) -> pd.DataFrame:
        """
        Fetch full options chain for today (or snapshot_date) including Greeks.

        Steps:
          1. GET /option-chains/{symbol}/nested  → expirations + strikes + streamer symbols
          2. Batch GET /market-data/by-type      → bid/ask/last/volume/OI
          3. DXLink Greeks subscription          → delta/gamma/theta/vega/rho/IV
        """
        symbol = symbol.upper()
        logger.info(f"[tastytrade] Fetching options chain for {symbol}")

        # ── 1. Chain structure ─────────────────────────────────────────────
        chain_resp = self._get(f"/option-chains/{symbol}/nested")
        chain_items = chain_resp.get("data", {}).get("items", [])
        if not chain_items:
            logger.warning(f"[tastytrade] No options chain data for {symbol}")
            return pd.DataFrame()

        # Build a flat list of {expiration, strike, option_type, occ_symbol, streamer_symbol}
        contracts: list[dict] = []
        for chain_item in chain_items:
            nested = chain_item.get("option-chain", {}).get("items", [])
            for exp_group in nested:
                exp_date_str  = exp_group.get("expiration-date", "")
                try:
                    exp_date = date.fromisoformat(exp_date_str)
                except ValueError:
                    continue
                strikes = exp_group.get("strikes", {}).get("items", [])
                for strike_item in strikes:
                    strike = float(strike_item.get("strike-price", 0))
                    for side in ("call", "put"):
                        occ = strike_item.get(side)
                        streamer = strike_item.get(f"{side}-streamer-symbol")
                        if occ and streamer:
                            contracts.append({
                                "expiration_date": exp_date,
                                "strike":          strike,
                                "option_type":     side,
                                "occ_symbol":      occ,
                                "streamer_symbol": streamer,
                            })

        if not contracts:
            logger.warning(f"[tastytrade] Chain structure empty for {symbol}")
            return pd.DataFrame()

        logger.info(f"[tastytrade] {len(contracts)} contracts found for {symbol}")

        # ── 2. Market data (bid/ask/last/volume/OI) in batches ────────────
        market_data: dict[str, dict] = {}
        occ_symbols = [c["occ_symbol"] for c in contracts]
        for i in range(0, len(occ_symbols), MARKET_DATA_BATCH):
            batch = occ_symbols[i : i + MARKET_DATA_BATCH]
            try:
                md_resp = self._get(
                    "/market-data/by-type",
                    params={"equity-option[]": batch},
                )
                for item in md_resp.get("data", {}).get("items", []):
                    market_data[item.get("symbol", "")] = item
            except Exception as e:
                logger.warning(f"[tastytrade] market-data/by-type batch failed: {e}")

        # ── 3. Greeks via DXLink ──────────────────────────────────────────
        streamer_syms  = [c["streamer_symbol"] for c in contracts]
        greeks_by_sym: dict[str, dict] = {}
        try:
            token_resp   = self._get("/api-quote-tokens")
            dxlink_url   = token_resp["data"]["dxlink-url"]
            dxlink_token = token_resp["data"]["token"]

            subs = [{"type": "Greeks", "symbol": s} for s in streamer_syms]
            raw_greeks = asyncio.run(_dxlink_fetch(
                url=dxlink_url,
                token=dxlink_token,
                subscriptions=subs,
                event_fields={"Greeks": GREEKS_FIELDS},
                target_event="Greeks",
                idle_timeout=DXLINK_IDLE_TIMEOUT,
            ))
            for g in raw_greeks:
                greeks_by_sym[g.get("eventSymbol", "")] = g
            logger.info(f"[tastytrade] Got Greeks for {len(greeks_by_sym)}/{len(contracts)} contracts")
        except Exception as e:
            logger.warning(f"[tastytrade] Greeks fetch failed (continuing without): {e}")

        # ── 4. Assemble DataFrame ──────────────────────────────────────────
        snap_at = datetime.combine(
            snapshot_date or date.today(), datetime.min.time()
        ).replace(tzinfo=timezone.utc)

        rows = []
        for c in contracts:
            occ      = c["occ_symbol"]
            streamer = c["streamer_symbol"]
            md       = market_data.get(occ, {})
            gk       = greeks_by_sym.get(streamer, {})

            def _f(val) -> Optional[float]:
                try:
                    v = float(val)
                    return None if v != v else v
                except (TypeError, ValueError):
                    return None

            def _i(val) -> Optional[int]:
                try:
                    return int(float(val))
                except (TypeError, ValueError):
                    return None

            rows.append({
                "snapshot_at":        snap_at,
                "symbol":             symbol,
                "expiration_date":    c["expiration_date"],
                "strike":             c["strike"],
                "option_type":        c["option_type"],
                "bid":                _f(md.get("bid")),
                "ask":                _f(md.get("ask")),
                "last":               _f(md.get("last")),
                "volume":             _i(md.get("volume")),
                "open_interest":      _i(md.get("open-interest")),
                "implied_volatility": _f(gk.get("volatility")),
                "delta":              _f(gk.get("delta")),
                "gamma":              _f(gk.get("gamma")),
                "theta":              _f(gk.get("theta")),
                "vega":               _f(gk.get("vega")),
                "rho":                _f(gk.get("rho")),
                "underlying_price":   None,
                "iv_percentile":      None,
                "iv_rank":            None,
                "provider":           self.name,
            })

        df = pd.DataFrame(rows)
        df = df.sort_values(["expiration_date", "strike", "option_type"]).reset_index(drop=True)
        logger.info(f"[tastytrade] Options chain assembled: {len(df)} rows")
        return df

    # ── Auth & HTTP helpers ────────────────────────────────────────────────

    def _ensure_token(self) -> str:
        """Return a valid access token, refreshing if within 60s of expiry."""
        if self._access_token and time.time() < self._token_expires_at - 60:
            return self._access_token

        logger.debug("[tastytrade] Refreshing OAuth2 access token")
        resp = httpx.post(
            f"{self._base_url}/oauth/token",
            data={
                "grant_type":    "refresh_token",
                "client_id":     self._client_id,
                "client_secret": self._client_secret,
                "refresh_token": self._refresh_token,
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=15,
        )
        resp.raise_for_status()
        body = resp.json()
        if "access_token" not in body:
            raise RuntimeError(f"Token refresh failed: {body}")
        self._access_token     = body["access_token"]
        self._token_expires_at = time.time() + body.get("expires_in", 900)
        logger.debug("[tastytrade] Token refreshed, expires in %ds", body.get("expires_in", 900))
        return self._access_token

    def _auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._ensure_token()}"}

    def _get(self, path: str, params: dict | None = None) -> dict:
        url = f"{self._base_url}{path}"
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            resp = client.get(url, params=params, headers=self._auth_headers())
            resp.raise_for_status()
            return resp.json()


# ── DXLink streaming helper (module-level, reusable) ──────────────────────────

async def _dxlink_fetch(
    url: str,
    token: str,
    subscriptions: list[dict],
    event_fields: dict[str, list[str]],
    target_event: str,
    idle_timeout: float = 8.0,
) -> list[dict]:
    """
    Connect to a DXLink WebSocket endpoint, subscribe to events, collect
    all records until the stream goes idle, then disconnect.

    Args:
        url:           DXLink WebSocket URL (wss://...)
        token:         DXLink auth token (from /api-quote-tokens)
        subscriptions: List of subscription dicts, e.g.
                       [{"type": "Candle", "symbol": "SPY{=1d}", "fromTime": 1704067200000}]
        event_fields:  Dict mapping event type to requested field names.
                       e.g. {"Candle": ["eventSymbol", "time", "open", ...]}
        target_event:  Event type to collect ("Candle" | "Greeks" | "Quote")
        idle_timeout:  Seconds with no new data before returning.

    Returns:
        List of dicts, one per event record, keyed by event_fields names.
    """
    import websockets  # imported here so the module loads without it

    field_order: list[str] = []
    collected:   list[dict] = []
    channel = 1

    try:
        async with websockets.connect(url, open_timeout=15) as ws:
            # ── Handshake ──────────────────────────────────────────────────
            await ws.send(json.dumps({
                "type": "SETUP", "channel": 0,
                "keepaliveTimeout": 60, "acceptKeepaliveTimeout": 60,
                "version": "0.1",
            }))
            await ws.send(json.dumps({
                "type": "AUTH", "channel": 0, "token": token,
            }))
            await ws.send(json.dumps({
                "type": "CHANNEL_REQUEST", "channel": channel,
                "service": "FEED", "parameters": {"contract": "AUTO"},
            }))
            await ws.send(json.dumps({
                "type": "FEED_SETUP", "channel": channel,
                "acceptAggregationPeriod": 0.1,
                "acceptDataFormat": "COMPACT",
                "acceptEventFields": event_fields,
            }))
            await ws.send(json.dumps({
                "type": "FEED_SUBSCRIPTION", "channel": channel,
                "reset": True, "add": subscriptions,
            }))

            # ── Receive loop ───────────────────────────────────────────────
            while True:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=idle_timeout)
                except asyncio.TimeoutError:
                    logger.debug(
                        f"[dxlink] Idle timeout after {idle_timeout}s — "
                        f"collected {len(collected)} {target_event} records"
                    )
                    break

                msg = json.loads(raw)
                msg_type = msg.get("type", "")

                if msg_type == "KEEPALIVE":
                    await ws.send(json.dumps({"type": "KEEPALIVE", "channel": 0}))

                elif msg_type == "FEED_CONFIG":
                    # Server confirms the field order for compact data
                    ef = msg.get("eventFields", {})
                    if target_event in ef:
                        field_order = ef[target_event]
                        logger.debug(f"[dxlink] field_order for {target_event}: {field_order}")

                elif msg_type == "FEED_DATA" and msg.get("channel") == channel:
                    payload = msg.get("data", [])
                    if not payload or payload[0] != target_event:
                        continue
                    fields = field_order or event_fields.get(target_event, [])
                    if not fields:
                        continue
                    # DXLink COMPACT format:
                    #   ["EventType", [val, val, ..., val, val, ...]]
                    # The inner list is all records concatenated (n fields each).
                    raw = payload[1] if len(payload) > 1 and isinstance(payload[1], list) else payload[1:]
                    n = len(fields)
                    for i in range(0, len(raw), n):
                        chunk = raw[i : i + n]
                        if len(chunk) < n:
                            break
                        collected.append(dict(zip(fields, chunk)))

                elif msg_type == "ERROR":
                    logger.error(f"[dxlink] Server error: {msg}")
                    break

    except Exception as e:
        logger.error(f"[dxlink] WebSocket error: {e}", exc_info=True)

    logger.info(f"[dxlink] Collected {len(collected)} {target_event} records")
    return collected
