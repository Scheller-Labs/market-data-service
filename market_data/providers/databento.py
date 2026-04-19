"""
market_data/providers/databento.py
Databento adapter — tick data, futures OHLCV, options, high-quality equities.
Paid, usage-based. Minimize calls — batch aggressively, archive everything.
"""

import json as _json
import logging
import math
import pathlib as _pathlib
from datetime import date, datetime, timedelta, timezone
from typing import Optional

import pandas as pd

from market_data.config import settings
from market_data.models import DataType, Interval
from market_data.providers.base import BaseProvider, RateLimitConfig

logger = logging.getLogger(__name__)

_CONFIRMED_SYMBOLS_PATH = _pathlib.Path(__file__).resolve().parents[4] / "data" / "databento_confirmed_symbols.json"


class DatabentoConfirmationRequired(Exception):
    """Raised when a symbol has not been confirmed for Databento data download."""
    def __init__(self, symbol: str):
        self.symbol = symbol
        super().__init__(
            f"Symbol '{symbol}' has not been confirmed for Databento download. "
            f"Add it to data/databento_confirmed_symbols.json or set "
            f"DATABENTO_REQUIRE_CONFIRMATION=false"
        )


def _check_databento_confirmation(symbol: str) -> None:
    """
    Raise DatabentoConfirmationRequired if the symbol has not been confirmed
    and DATABENTO_REQUIRE_CONFIRMATION is not explicitly 'false'.
    """
    import os
    if os.environ.get("DATABENTO_REQUIRE_CONFIRMATION", "true").lower() == "false":
        return  # Gate disabled
    try:
        data = _json.loads(_CONFIRMED_SYMBOLS_PATH.read_text())
        confirmed = {s.upper() for s in data.get("confirmed_symbols", [])}
        if symbol.upper() not in confirmed:
            raise DatabentoConfirmationRequired(symbol)
    except DatabentoConfirmationRequired:
        raise
    except Exception:
        # If the file doesn't exist or can't be read, allow the call through
        # (fail-open to not break existing functionality)
        pass


# ── Black-Scholes IV solver (no scipy — pure stdlib math) ─────────────────

def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf (stdlib, no external deps)."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def _bs_price(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    """Black-Scholes option price."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(0.0, (S - K) if is_call else (K - S))
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    if is_call:
        return S * _norm_cdf(d1) - K * math.exp(-r * T) * _norm_cdf(d2)
    return K * math.exp(-r * T) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


def _bs_vega(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes vega (identical for calls and puts)."""
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    sqrt_T = math.sqrt(T)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    return S * sqrt_T * math.exp(-0.5 * d1 * d1) / math.sqrt(2.0 * math.pi)


def implied_vol(
    market_price: float,
    S: float,
    K: float,
    T: float,
    r: float,
    is_call: bool,
    max_iter: int = 60,
    tol: float = 1e-5,
) -> Optional[float]:
    """
    Newton-Raphson implied volatility solver.

    Args:
        market_price: Observed option close price.
        S: Underlying spot price.
        K: Strike price.
        T: Time to expiration in years.
        r: Risk-free rate (e.g. 0.05 for 5%).
        is_call: True for call, False for put.

    Returns:
        Implied volatility as a decimal (e.g. 0.18 for 18%), or None if
        convergence fails or inputs are invalid.
    """
    if T <= 0 or market_price <= 0 or S <= 0 or K <= 0:
        return None

    # Price must exceed intrinsic value (add tiny tolerance for rounding)
    intrinsic = max(0.0, (S - K) if is_call else (K - S))
    if market_price <= intrinsic + 0.001:
        return None

    sigma = 0.3  # initial guess: 30% vol
    for _ in range(max_iter):
        price = _bs_price(S, K, T, r, sigma, is_call)
        vega = _bs_vega(S, K, T, r, sigma)
        diff = market_price - price
        if abs(diff) < tol:
            return max(0.001, min(sigma, 5.0))
        if abs(vega) < 1e-10:
            break
        sigma += diff / vega
        sigma = max(0.001, min(sigma, 5.0))  # bound: 0.1%–500% vol

    # Accept if close enough on the final iteration
    if abs(_bs_price(S, K, T, r, sigma, is_call) - market_price) < 0.02:
        return max(0.001, min(sigma, 5.0))
    return None


def compute_atm_iv_from_opra(
    defs_df: pd.DataFrame,
    ohlcv_df: pd.DataFrame,
    underlying_price: float,
    snap: date,
    r: float = 0.05,
) -> Optional[float]:
    """
    Compute ATM implied volatility from OPRA.PILLAR definition + ohlcv-1d data.

    Strategy:
    - Select the nearest option expiry with 7–60 DTE.
    - Keep near-ATM strikes (within ±5% of the underlying spot price).
    - Require a positive close price (option actually traded).
    - Solve for IV via Black-Scholes Newton-Raphson for each qualifying contract.
    - Return the median IV across all qualifying contracts.

    Args:
        defs_df:          OPRA definition DataFrame (from Databento definition schema).
        ohlcv_df:         OPRA ohlcv-1d DataFrame for the same date.
        underlying_price: Current spot price of the underlying.
        snap:             The snapshot date (used for DTE computation).
        r:                Risk-free rate assumption (default 5%).

    Returns:
        Median ATM IV as a decimal (e.g. 0.18 for 18%), or None if not computable.
    """
    if defs_df.empty or underlying_price <= 0:
        return None

    S = underlying_price
    defs = defs_df.copy()

    defs["_expiry"]  = pd.to_datetime(defs["expiration"], utc=True).dt.date
    defs["_is_call"] = defs["instrument_class"] == "C"
    defs["_strike"]  = defs["strike_price"].astype(float)
    defs["_dte"]     = defs["_expiry"].apply(lambda e: (e - snap).days)

    # Filter: 7–60 DTE
    defs = defs[(defs["_dte"] >= 7) & (defs["_dte"] <= 60)]
    if defs.empty:
        return None

    # Pick the nearest expiry
    nearest_dte = int(defs["_dte"].min())
    defs = defs[defs["_dte"] == nearest_dte]

    # Near-ATM: strikes within ±5% of spot
    defs = defs[(defs["_strike"] >= S * 0.95) & (defs["_strike"] <= S * 1.05)]
    if defs.empty:
        return None

    # Build close-price lookup from ohlcv_df
    ohlcv_by_id: dict[int, float] = {}
    if not ohlcv_df.empty and "instrument_id" in ohlcv_df.columns and "close" in ohlcv_df.columns:
        best = ohlcv_df.sort_values("volume").groupby("instrument_id").last()
        for iid, row in best.iterrows():
            c = row["close"]
            if pd.notna(c) and float(c) > 0:
                ohlcv_by_id[int(iid)] = float(c)

    T = nearest_dte / 365.0
    ivs: list[float] = []

    for _, row in defs.iterrows():
        iid = int(row["instrument_id"])
        close_price = ohlcv_by_id.get(iid)
        if close_price is None:
            continue
        K = float(row["_strike"])
        iv = implied_vol(close_price, S, K, T, r, bool(row["_is_call"]))
        if iv is not None and 0.01 <= iv <= 3.0:  # sanity: 1%–300% vol
            ivs.append(iv)

    if not ivs:
        return None

    ivs.sort()
    mid = len(ivs) // 2
    return ivs[mid] if len(ivs) % 2 == 1 else (ivs[mid - 1] + ivs[mid]) / 2.0

# Databento dataset mappings
DATASET_EQUITIES = "XNAS.ITCH"      # Nasdaq equities
DATASET_OPTIONS  = "OPRA.PILLAR"    # Options Price Reporting Authority
DATASET_FUTURES  = "GLBX.MDP3"     # CME futures

DATABENTO_SCHEMA_MAP = {
    "ohlcv_1d":  "ohlcv-1d",
    "ohlcv_1h":  "ohlcv-1h",
    "ohlcv_1m":  "ohlcv-1m",
    "trades":    "trades",
    "mbp_1":     "mbp-1",      # Level 1 market-by-price
    "mbp_10":    "mbp-10",     # Level 2 market-by-price
}


class DatabentoProvider(BaseProvider):
    name = "databento"

    def __init__(self):
        self.api_key = settings.databento_api_key
        if not self.api_key:
            logger.warning("DATABENTO_API_KEY not set — provider will fail on requests")
        self._client = None
        super().__init__()

    def _get_client(self):
        """Lazy-init Databento client to avoid import errors when key not set."""
        if self._client is None:
            try:
                import databento as db
                self._client = db.Historical(key=self.api_key)
            except ImportError:
                raise RuntimeError(
                    "databento package not installed. Run: pip install databento"
                )
        return self._client

    def get_rate_limit_config(self) -> RateLimitConfig:
        # Databento is usage-based / billed per byte — no hard rate limit,
        # but we enforce a conservative minimum to avoid accidental bulk charges
        return RateLimitConfig(
            min_interval_seconds=0.5,
        )

    def supported_data_types(self) -> list[DataType]:
        return [
            DataType.OHLCV,
            DataType.OHLCV_INTRADAY,
            DataType.OPTIONS_CHAIN,
            DataType.IV_RANK,
            DataType.TICK,
            DataType.FUTURES_OHLCV,
        ]

    # ── OHLCV ──────────────────────────────────────────────────────────────

    def _fetch_ohlcv(self, symbol: str, start: date, end: date, interval: Interval) -> pd.DataFrame:
        _check_databento_confirmation(symbol)
        schema = self._interval_to_schema(interval)
        client = self._get_client()

        logger.info(f"[databento] Fetching {schema} for {symbol} {start}→{end}")
        data = client.timeseries.get_range(
            dataset=DATASET_EQUITIES,
            symbols=[symbol.upper()],
            schema=schema,
            start=start.isoformat(),
            end=end.isoformat(),
        )
        df = data.to_df()
        if df.empty:
            return df

        return self._normalize_ohlcv(df, symbol)

    def _normalize_ohlcv(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Normalize Databento OHLCV DataFrame to MDS schema."""
        # Databento uses nanosecond timestamps in 'ts_event'
        df = df.copy()
        if "ts_event" in df.columns:
            df["timestamp"] = pd.to_datetime(df["ts_event"], utc=True)
        elif df.index.name == "ts_event":
            df["timestamp"] = pd.to_datetime(df.index, utc=True)
            df = df.reset_index(drop=True)

        # Databento price fields are in fixed-point integer (1e-9 scaling)
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = df[col] / 1e9

        rename_map = {"size": "volume"}
        df = df.rename(columns=rename_map)
        return self._enforce_ohlcv_schema(df, symbol)

    # ── Tick Data ──────────────────────────────────────────────────────────

    def fetch_tick_data(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        """
        Fetch raw trade ticks. Returns large DataFrames — save to Parquet immediately.
        WARNING: This can generate very large data volumes and significant API costs.
        """
        _check_databento_confirmation(symbol)
        self._limiter.wait_if_needed()
        client = self._get_client()

        logger.info(f"[databento] Fetching tick data for {symbol} {start}→{end} — this may be large")
        data = client.timeseries.get_range(
            dataset=DATASET_EQUITIES,
            symbols=[symbol.upper()],
            schema="trades",
            start=start.isoformat(),
            end=end.isoformat(),
        )
        df = data.to_df()
        if df.empty:
            return df

        df = df.copy()
        if "ts_event" in df.columns:
            df["timestamp"] = pd.to_datetime(df["ts_event"], utc=True)
        if "price" in df.columns:
            df["price"] = df["price"] / 1e9
        df["symbol"] = symbol.upper()
        df["provider"] = self.name
        return df

    # ── Futures OHLCV ──────────────────────────────────────────────────────

    def fetch_futures_ohlcv(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: Interval = Interval.ONE_DAY,
    ) -> pd.DataFrame:
        """Fetch futures OHLCV from CME dataset."""
        _check_databento_confirmation(symbol)
        self._limiter.wait_if_needed()
        schema = self._interval_to_schema(interval)
        client = self._get_client()

        data = client.timeseries.get_range(
            dataset=DATASET_FUTURES,
            symbols=[symbol.upper()],
            schema=schema,
            start=start.isoformat(),
            end=end.isoformat(),
        )
        df = data.to_df()
        if df.empty:
            return df
        return self._normalize_ohlcv(df, symbol)

    # ── Options Chain ─────────────────────────────────────────────────────

    def _fetch_options_chain(self, symbol: str, snapshot_date: Optional[date]) -> pd.DataFrame:
        """
        Fetch a historical options chain snapshot from Databento OPRA.PILLAR.

        Uses three requests against the target date:
          1. definition schema  → instrument_id → strike / expiration / call|put
          2. ohlcv-1d schema    → close (last) + volume per instrument
          3. statistics schema  → open interest (stat_type=9) per instrument

        Notes:
          - OPRA.PILLAR parent symbology requires "{SYMBOL}.OPT" (e.g. "SPY.OPT").
          - to_df() auto-converts fixed-point prices to float dollars — no /1e9 needed.
          - ohlcv-1d has one row per exchange per instrument; we keep the row with
            the highest volume as the representative end-of-day print.
          - statistics has many duplicate OI rows (one per exchange reporter); we
            keep the last non-null quantity per instrument_id.
          - OPRA.PILLAR carries only OI in statistics. Greeks and IV are NULL;
            supplement with TastyTrade or compute from option pricing models.

        Args:
            symbol:        Underlying ticker, e.g. "SPY".
            snapshot_date: Date of the historical snapshot.  Defaults to today.

        Returns:
            DataFrame matching the MDS options_snapshots schema.
        """
        _check_databento_confirmation(symbol)
        self._limiter.wait_if_needed()
        client = self._get_client()

        # OPRA.PILLAR historical data has a ~10-minute processing lag and is only
        # fully available for completed trading days.  When no explicit snapshot_date
        # is requested (i.e. "fetch current"), step back to yesterday to stay within
        # the available range.  Explicit historical dates are passed through unchanged.
        if snapshot_date is None:
            snap = date.today() - timedelta(days=1)
        else:
            snap = snapshot_date

        # Databento end is exclusive → request [snap, snap+1)
        start_str  = snap.isoformat()
        end_str    = (snap + timedelta(days=1)).isoformat()
        sym_upper  = symbol.upper()
        # OPRA.PILLAR parent symbology requires "{ROOT}.OPT" suffix
        parent_sym = f"{sym_upper}.OPT"

        logger.info(f"[databento] Fetching options chain {sym_upper} ({parent_sym}) for {snap}")

        # ── 1. Instrument definitions ──────────────────────────────────────
        try:
            defs_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="definition",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as e:
            logger.error(f"[databento] definition fetch failed for {sym_upper}: {e}")
            return pd.DataFrame()

        if defs_df.empty:
            logger.warning(f"[databento] No option definitions for {sym_upper} on {snap}")
            return pd.DataFrame()

        # Keep only option contracts (instrument_class 'C' or 'P')
        defs_df = defs_df[defs_df["instrument_class"].isin(["C", "P"])].copy()
        if defs_df.empty:
            logger.warning(f"[databento] No option contracts in definitions for {sym_upper}")
            return pd.DataFrame()

        # Multiple definition updates can arrive for the same instrument_id;
        # keep the last one (most recent update wins).
        defs_df = defs_df.drop_duplicates(subset="instrument_id", keep="last")

        # to_df() already converts strike_price to float dollars.
        # expiration is a tz-aware pandas Timestamp — extract just the date portion.
        defs_df["_expiry"]      = pd.to_datetime(defs_df["expiration"], utc=True).dt.date
        defs_df["_option_type"] = defs_df["instrument_class"].map({"C": "call", "P": "put"})

        defs_idx: dict[int, dict] = (
            defs_df.set_index("instrument_id")[["strike_price", "_expiry", "_option_type"]]
            .rename(columns={"strike_price": "_strike"})
            .to_dict("index")
        )
        logger.info(f"[databento] {len(defs_idx)} option contracts in definitions")

        # ── 2. Daily OHLCV — close price (last) + volume ──────────────────
        # ohlcv-1d returns one row per exchange per instrument_id.
        # Collapse to the row with the highest volume (best representative print).
        ohlcv_by_id: dict[int, dict] = {}
        try:
            ohlcv_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="ohlcv-1d",
                start=start_str,
                end=end_str,
            ).to_df()
            if not ohlcv_df.empty:
                best = ohlcv_df.sort_values("volume").groupby("instrument_id").last()
                for iid, row in best.iterrows():
                    ohlcv_by_id[int(iid)] = {
                        "last":   float(row["close"]),
                        "volume": int(row["volume"]) if pd.notna(row["volume"]) else 0,
                    }
                logger.info(f"[databento] OHLCV instruments: {len(ohlcv_by_id)}")
        except Exception as e:
            logger.warning(f"[databento] ohlcv-1d fetch failed (continuing): {e}")

        # ── 3. Statistics — open interest ──────────────────────────────────
        # OPRA.PILLAR statistics carries only stat_type=9 (OPEN_INTEREST).
        # quantity field holds the OI count; price is NaN for OI records.
        # Many duplicate rows arrive (one per exchange reporter); keep the last
        # non-null quantity per instrument_id.
        STAT_OI = 9
        oi_by_id: dict[int, int] = {}
        try:
            stats_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="statistics",
                start=start_str,
                end=end_str,
            ).to_df()
            if not stats_df.empty and "stat_type" in stats_df.columns:
                oi_df = stats_df[
                    (stats_df["stat_type"] == STAT_OI) &
                    stats_df["quantity"].notna()
                ]
                for iid, grp in oi_df.groupby("instrument_id"):
                    oi_by_id[int(iid)] = int(grp["quantity"].iloc[-1])
                logger.info(f"[databento] OI instruments: {len(oi_by_id)}")
        except Exception as e:
            logger.warning(f"[databento] statistics fetch failed (continuing): {e}")

        # ── 4. Assemble rows ───────────────────────────────────────────────
        snap_at = datetime.combine(snap, datetime.min.time()).replace(tzinfo=timezone.utc)
        rows = []
        for iid, defn in defs_idx.items():
            ohlcv = ohlcv_by_id.get(iid, {})
            rows.append({
                "snapshot_at":        snap_at,
                "symbol":             sym_upper,
                "expiration_date":    defn["_expiry"],
                "strike":             defn["_strike"],
                "option_type":        defn["_option_type"],
                "bid":                None,
                "ask":                None,
                "last":               ohlcv.get("last"),
                "volume":             ohlcv.get("volume"),
                "open_interest":      oi_by_id.get(iid),
                "implied_volatility": None,
                "delta":              None,
                "gamma":              None,
                "theta":              None,
                "vega":               None,
                "rho":                None,
                "underlying_price":   None,
                "iv_rank":            None,
                "iv_percentile":      None,
                "provider":           self.name,
            })

        if not rows:
            logger.warning(f"[databento] No rows assembled for {sym_upper}")
            return pd.DataFrame()

        df = (
            pd.DataFrame(rows)
            .sort_values(["expiration_date", "strike", "option_type"])
            .reset_index(drop=True)
        )
        logger.info(f"[databento] Options chain assembled: {len(df)} rows for {sym_upper} on {snap}")
        return df

    # ── IV Rank (ATM IV from OPRA.PILLAR via Black-Scholes) ───────────────

    def _fetch_iv_rank(self, symbol: str) -> pd.DataFrame:
        """
        Fetch today's ATM implied volatility from Databento OPRA.PILLAR.

        Flow:
          1. Fetch the last 5 days of underlying OHLCV from XNAS.ITCH to get the
             most recent close price (used for ATM strike selection).
          2. Fetch today's OPRA.PILLAR definition + ohlcv-1d for the options chain.
          3. Compute ATM IV via Black-Scholes Newton-Raphson (see compute_atm_iv_from_opra).

        Returns a single-row DataFrame: recorded_at, symbol, current_iv, provider.
        The caller (MarketDataService.get_iv_rank) computes iv_rank / iv_percentile
        from stored history and upserts the complete row.

        Note: XNAS.ITCH covers Nasdaq-listed equities. For NYSE-listed underlyings
        (e.g. SPY on NYSE Arca) this call may return empty; the service layer will
        fall back to the Finnhub provider automatically.
        """
        _check_databento_confirmation(symbol)
        self._limiter.wait_if_needed()
        client = self._get_client()
        today = date.today()
        sym = symbol.upper()
        parent_sym = f"{sym}.OPT"

        # Step 1: underlying close price (last 5 trading days, take most recent)
        underlying_price: Optional[float] = None
        try:
            eq_data = client.timeseries.get_range(
                dataset=DATASET_EQUITIES,
                symbols=[sym],
                schema="ohlcv-1d",
                start=(today - timedelta(days=7)).isoformat(),
                end=(today + timedelta(days=1)).isoformat(),
            ).to_df()
            if not eq_data.empty:
                underlying_price = float(eq_data.iloc[-1]["close"]) / 1e9
        except Exception as exc:
            logger.warning("[databento] Could not fetch underlying price for %s: %s", sym, exc)

        if not underlying_price:
            logger.warning(
                "[databento] No underlying price for %s — cannot compute ATM IV. "
                "XNAS.ITCH only covers Nasdaq-listed instruments; use Finnhub for NYSE/Arca.",
                sym,
            )
            return pd.DataFrame()

        # Step 2: options chain (definition + ohlcv-1d) for today
        start_str = today.isoformat()
        end_str   = (today + timedelta(days=1)).isoformat()

        try:
            defs_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="definition",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as exc:
            logger.error("[databento] IV rank definition fetch failed for %s: %s", sym, exc)
            return pd.DataFrame()

        if defs_df.empty:
            logger.warning("[databento] No options definitions for %s on %s", sym, today)
            return pd.DataFrame()

        defs_df = defs_df[defs_df["instrument_class"].isin(["C", "P"])].copy()
        defs_df = defs_df.drop_duplicates(subset="instrument_id", keep="last")

        try:
            ohlcv_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="ohlcv-1d",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as exc:
            logger.warning("[databento] IV rank ohlcv-1d failed for %s: %s", sym, exc)
            ohlcv_df = pd.DataFrame()

        # Step 3: compute ATM IV
        atm_iv = compute_atm_iv_from_opra(defs_df, ohlcv_df, underlying_price, today)
        if atm_iv is None:
            logger.warning("[databento] Could not compute ATM IV for %s on %s", sym, today)
            return pd.DataFrame()

        logger.info("[databento] ATM IV for %s: %.4f", sym, atm_iv)
        return pd.DataFrame([{
            "recorded_at": today,
            "symbol":      sym,
            "current_iv":  round(atm_iv, 6),
            "provider":    self.name,
        }])

    # ── ATM IV with externally-supplied underlying price ──────────────────

    def fetch_atm_iv_with_spot(
        self,
        symbol: str,
        underlying_price: float,
        snap: Optional[date] = None,
    ) -> Optional[float]:
        """
        Compute ATM implied volatility from OPRA.PILLAR using a caller-supplied
        underlying spot price (bypasses the XNAS.ITCH equity feed entirely).

        Useful when the underlying is NYSE/Arca-listed (e.g. SPY, QQQ) where
        XNAS.ITCH returns no data, but the spot price is available from another
        source (e.g. the OHLCV store from TastyTrade or Alpha Vantage).

        Returns the ATM IV as a decimal (e.g. 0.18 for 18%), or None on failure.
        """
        _check_databento_confirmation(symbol)
        if underlying_price <= 0:
            return None

        target = snap or (date.today() - timedelta(days=1))
        # Clamp to yesterday so we never exceed the OPRA available range
        if target >= date.today():
            target = date.today() - timedelta(days=1)

        start_str = target.isoformat()
        end_str   = (target + timedelta(days=1)).isoformat()
        parent_sym = f"{symbol.upper()}.OPT"

        self._limiter.wait_if_needed()
        client = self._get_client()

        try:
            defs_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="definition",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as exc:
            logger.warning("[databento] fetch_atm_iv_with_spot definition failed for %s: %s", symbol, exc)
            return None

        if defs_df.empty:
            return None

        defs_df = defs_df[defs_df["instrument_class"].isin(["C", "P"])].copy()
        defs_df = defs_df.drop_duplicates(subset="instrument_id", keep="last")

        try:
            ohlcv_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="ohlcv-1d",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as exc:
            logger.warning("[databento] fetch_atm_iv_with_spot ohlcv-1d failed for %s: %s", symbol, exc)
            ohlcv_df = pd.DataFrame()

        atm_iv = compute_atm_iv_from_opra(defs_df, ohlcv_df, underlying_price, target)
        logger.info("[databento] fetch_atm_iv_with_spot %s: spot=%.2f iv=%s", symbol, underlying_price, atm_iv)
        return atm_iv

    # ── Health Check ──────────────────────────────────────────────────────

    def _health_check(self) -> bool:
        try:
            client = self._get_client()
            # Lightweight metadata call
            client.metadata.get_dataset_range(dataset=DATASET_EQUITIES)
            return True
        except Exception as e:
            logger.warning(f"[databento] Health check failed: {e}")
            return False

    # ── Helpers ───────────────────────────────────────────────────────────

    def _interval_to_schema(self, interval: Interval) -> str:
        map_ = {
            Interval.ONE_MIN:    "ohlcv-1m",
            Interval.FIVE_MIN:   "ohlcv-1m",    # resample after fetch
            Interval.FIFTEEN_MIN:"ohlcv-1m",
            Interval.ONE_HOUR:   "ohlcv-1h",
            Interval.ONE_DAY:    "ohlcv-1d",
            Interval.ONE_WEEK:   "ohlcv-1d",    # resample after fetch
        }
        return map_.get(interval, "ohlcv-1d")
