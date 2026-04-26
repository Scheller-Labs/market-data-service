"""
market_data/providers/finnhub.py
Finnhub adapter — OHLCV, options chain, IV, news/sentiment, earnings.
Free tier: 60 calls/min. Strong options and sentiment coverage.
"""

import logging
from datetime import date, datetime, timezone, timedelta
from typing import Optional

import httpx
import pandas as pd

from market_data.config import settings
from market_data.models import DataType, Interval, OptionType
from market_data.providers.base import BaseProvider, RateLimitConfig

logger = logging.getLogger(__name__)

BASE_URL = "https://finnhub.io/api/v1"

FINNHUB_RESOLUTION_MAP = {
    Interval.ONE_MIN:     "1",
    Interval.FIVE_MIN:    "5",
    Interval.FIFTEEN_MIN: "15",
    Interval.ONE_HOUR:    "60",
    Interval.ONE_DAY:     "D",
    Interval.ONE_WEEK:    "W",
}


class FinnhubProvider(BaseProvider):
    name = "finnhub"

    def __init__(self):
        self.api_key = settings.finnhub_api_key
        if not self.api_key:
            logger.warning("FINNHUB_API_KEY not set — provider will fail on requests")
        super().__init__()

    def get_rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(
            calls_per_minute=60,
            min_interval_seconds=1.1,   # slight buffer over 1s
        )

    def supported_data_types(self) -> list[DataType]:
        return [
            DataType.OHLCV,
            DataType.OHLCV_INTRADAY,
            DataType.OPTIONS_CHAIN,
            DataType.NEWS_SENTIMENT,
            DataType.EARNINGS,
            DataType.FUNDAMENTALS,
            DataType.IV_RANK,
        ]

    # ── OHLCV ──────────────────────────────────────────────────────────────

    def _fetch_ohlcv(self, symbol: str, start: date, end: date, interval: Interval) -> pd.DataFrame:
        resolution = FINNHUB_RESOLUTION_MAP.get(interval, "D")
        start_ts = int(datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp())
        end_ts   = int(datetime.combine(end, datetime.max.time()).replace(tzinfo=timezone.utc).timestamp())

        data = self._get("/stock/candle", {
            "symbol":     symbol.upper(),
            "resolution": resolution,
            "from":       start_ts,
            "to":         end_ts,
        })

        if data.get("s") == "no_data" or "t" not in data:
            return pd.DataFrame()

        rows = []
        for i, ts in enumerate(data["t"]):
            rows.append({
                "timestamp": datetime.fromtimestamp(ts, tz=timezone.utc),
                "open":      data["o"][i],
                "high":      data["h"][i],
                "low":       data["l"][i],
                "close":     data["c"][i],
                "volume":    int(data["v"][i]),
            })

        df = pd.DataFrame(rows).sort_values("timestamp").reset_index(drop=True)
        return self._enforce_ohlcv_schema(df, symbol)

    # ── Options Chain ──────────────────────────────────────────────────────

    def _fetch_options_chain(self, symbol: str, snapshot_date: Optional[date] = None) -> pd.DataFrame:
        params: dict = {"symbol": symbol.upper()}
        if snapshot_date:
            params["date"] = snapshot_date.isoformat()

        data = self._get("/stock/option-chain", params)
        now = datetime.now(tz=timezone.utc)
        rows = []

        for expiry_data in data.get("data", []):
            exp_date_str = expiry_data.get("expirationDate", "")
            try:
                exp_date = datetime.strptime(exp_date_str, "%Y-%m-%d").date()
            except ValueError:
                continue

            for opt in expiry_data.get("options", {}).get("CALL", []):
                rows.append(self._parse_option_row(opt, symbol, exp_date, OptionType.CALL, now))
            for opt in expiry_data.get("options", {}).get("PUT", []):
                rows.append(self._parse_option_row(opt, symbol, exp_date, OptionType.PUT, now))

        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def _parse_option_row(
        self, opt: dict, symbol: str, exp_date: date, option_type: OptionType, now: datetime
    ) -> dict:
        return {
            "snapshot_at":        now,
            "symbol":             symbol.upper(),
            "expiration_date":    exp_date,
            "strike":             float(opt.get("strike", 0)),
            "option_type":        option_type.value,
            "bid":                self._float(opt.get("bid")),
            "ask":                self._float(opt.get("ask")),
            "last":               self._float(opt.get("lastPrice")),
            "volume":             self._int(opt.get("volume")),
            "open_interest":      self._int(opt.get("openInterest")),
            "implied_volatility": self._float(opt.get("impliedVolatility")),
            "delta":              self._float(opt.get("delta")),
            "gamma":              self._float(opt.get("gamma")),
            "theta":              self._float(opt.get("theta")),
            "vega":               self._float(opt.get("vega")),
            "provider":           self.name,
        }

    # ── Fundamentals ───────────────────────────────────────────────────────

    def _fetch_fundamentals(self, symbol: str) -> pd.DataFrame:
        metric_data = self._get("/stock/metric", {"symbol": symbol, "metric": "all"})
        profile_data = self._get("/stock/profile2", {"symbol": symbol})
        metrics = metric_data.get("metric", {})
        today = date.today()

        row = {
            "snapshot_date":  today,
            "symbol":         symbol.upper(),
            "pe_ratio":       self._float(metrics.get("peBasicExclExtraTTM")),
            "eps":            self._float(metrics.get("epsBasicExclExtraAnnual")),
            "market_cap":     self._int(profile_data.get("marketCapitalization")),
            "roe":            self._float(metrics.get("roeTTM")),
            "sector":         profile_data.get("finnhubIndustry"),
            "industry":       profile_data.get("finnhubIndustry"),
            "raw_data":       {"metrics": metrics, "profile": profile_data},
            "provider":       self.name,
        }
        return pd.DataFrame([row])

    # ── Earnings ──────────────────────────────────────────────────────────

    def _fetch_earnings(self, symbol: str) -> pd.DataFrame:
        data = self._get("/stock/earnings", {"symbol": symbol})
        rows = []
        for item in data:
            rows.append({
                "report_date":    self._date(item.get("date")),
                "symbol":         symbol.upper(),
                "eps_actual":     self._float(item.get("actual")),
                "eps_estimate":   self._float(item.get("estimate")),
                "eps_surprise":   self._float(item.get("surprise")),
                "fiscal_quarter": item.get("period"),
                "provider":       self.name,
            })
        return pd.DataFrame(rows).dropna(subset=["report_date"])

    # ── News/Sentiment ─────────────────────────────────────────────────────

    def _fetch_news_sentiment(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        data = self._get("/company-news", {
            "symbol": symbol.upper(),
            "from":   start.isoformat(),
            "to":     end.isoformat(),
        })
        rows = []
        for item in data:
            ts = datetime.fromtimestamp(item.get("datetime", 0), tz=timezone.utc)
            rows.append({
                "published_at":    ts,
                "symbol":          symbol.upper(),
                "headline":        item.get("headline", ""),
                "source":          item.get("source", ""),
                "sentiment_score": None,
                "sentiment_label": None,
                "url":             item.get("url"),
                "provider":        self.name,
            })
        return pd.DataFrame(rows)

    # ── IV Rank ────────────────────────────────────────────────────────────

    def _fetch_iv_rank(self, symbol: str) -> pd.DataFrame:
        """
        Compute current ATM implied volatility using the Finnhub options chain.

        Steps:
          1. GET /quote to get current stock price (needed for ATM calculation)
          2. GET /stock/option-chain for all expirations
          3. Pick the nearest expiry with 7–60 DTE
          4. Average the IV of the two strikes closest to current price
             (one call + one put at each near-ATM strike)

        Returns a single-row DataFrame: recorded_at, symbol, current_iv, provider.
        Returns empty DataFrame if IV cannot be computed (no options data).

        Note: /stock/option-chain requires a Finnhub paid plan. The free-tier key
        will produce a 403 here; upgrade at finnhub.io/pricing to unlock it.
        """
        today = date.today()

        # Step 1: current price
        quote = self._get("/quote", {"symbol": symbol.upper()})
        current_price = self._float(quote.get("c"))
        if not current_price or current_price <= 0:
            logger.warning("[%s] Could not get current price for %s", self.name, symbol)
            return pd.DataFrame()

        # Step 2: options chain (paid endpoint — free tier returns 403)
        try:
            chain = self._get("/stock/option-chain", {"symbol": symbol.upper()})
        except Exception as exc:
            if "403" in str(exc):
                logger.warning(
                    "[finnhub] /stock/option-chain returned 403 for %s. "
                    "Options data requires a Finnhub paid plan (finnhub.io/pricing). "
                    "Falling back to stored options chain.",
                    symbol,
                )
            else:
                logger.warning("[finnhub] Could not fetch option chain for %s: %s", symbol, exc)
            return pd.DataFrame()

        atm_iv = self._compute_atm_iv(chain.get("data", []), current_price, today)
        if atm_iv is None:
            logger.warning("[%s] Could not compute ATM IV for %s", self.name, symbol)
            return pd.DataFrame()

        return pd.DataFrame([{
            "recorded_at": today,
            "symbol":       symbol.upper(),
            "current_iv":   round(atm_iv, 6),
            "provider":     self.name,
        }])

    def _compute_atm_iv(
        self, expiry_data: list, current_price: float, today: date
    ) -> Optional[float]:
        """
        Find the nearest expiry (7–60 DTE) and return the average IV of the
        three strikes nearest to current_price, combining calls and puts.
        """
        # Collect expirations in the 7–60 DTE window
        eligible: list[tuple[int, dict]] = []
        for expiry in expiry_data:
            exp_str = expiry.get("expirationDate", "")
            try:
                exp_date = datetime.strptime(exp_str, "%Y-%m-%d").date()
            except ValueError:
                continue
            dte = (exp_date - today).days
            if 7 <= dte <= 60:
                eligible.append((dte, expiry))

        if not eligible:
            return None

        # Use the nearest eligible expiry
        eligible.sort(key=lambda x: x[0])
        _, nearest = eligible[0]

        # Collect IV by strike for calls and puts
        strike_ivs: dict[float, list[float]] = {}
        for opt in nearest.get("options", {}).get("CALL", []):
            strike = self._float(opt.get("strike"))
            iv = self._float(opt.get("impliedVolatility"))
            if strike and iv and iv > 0:
                strike_ivs.setdefault(strike, []).append(iv)
        for opt in nearest.get("options", {}).get("PUT", []):
            strike = self._float(opt.get("strike"))
            iv = self._float(opt.get("impliedVolatility"))
            if strike and iv and iv > 0:
                strike_ivs.setdefault(strike, []).append(iv)

        if not strike_ivs:
            return None

        # Take the 3 strikes closest to current price
        sorted_strikes = sorted(strike_ivs, key=lambda s: abs(s - current_price))
        atm_ivs: list[float] = []
        for strike in sorted_strikes[:3]:
            atm_ivs.extend(strike_ivs[strike])

        return sum(atm_ivs) / len(atm_ivs) if atm_ivs else None

    # ── Health Check ──────────────────────────────────────────────────────

    def _health_check(self) -> bool:
        data = self._get("/quote", {"symbol": "AAPL"})
        return "c" in data  # current price field

    # ── HTTP Utility ──────────────────────────────────────────────────────

    def _get(self, path: str, params: dict) -> dict | list:
        params["token"] = self.api_key
        url = BASE_URL + path
        with httpx.Client(timeout=settings.request_timeout_seconds) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
        return resp.json()

    # ── Type Helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _float(val) -> Optional[float]:
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _int(val) -> Optional[int]:
        try:
            return int(float(val))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _date(val: Optional[str]) -> Optional[date]:
        if not val:
            return None
        try:
            return datetime.strptime(val, "%Y-%m-%d").date()
        except ValueError:
            return None
