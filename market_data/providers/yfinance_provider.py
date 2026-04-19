"""
market_data/providers/yfinance_provider.py
yfinance adapter — free, full historical daily OHLCV for US equities/ETFs.
Covers NYSE, Nasdaq, NYSE Arca — no API key required.
Rate limit: unofficial; ~2000 req/hour in practice. No intraday history.
"""

import logging
from datetime import date, datetime, timezone

import pandas as pd

from market_data.models import DataType, Interval
from market_data.providers.base import BaseProvider, RateLimitConfig

logger = logging.getLogger(__name__)


class YFinanceProvider(BaseProvider):
    name = "yfinance"

    def __init__(self):
        super().__init__()

    def get_rate_limit_config(self) -> RateLimitConfig:
        return RateLimitConfig(min_interval_seconds=0.5)

    def supported_data_types(self) -> list[DataType]:
        return [DataType.OHLCV]

    def _health_check(self) -> bool:
        try:
            import yfinance as yf
            ticker = yf.Ticker("SPY")
            hist = ticker.history(period="5d", auto_adjust=False)
            return not hist.empty
        except Exception as exc:
            logger.warning("yfinance health check failed: %s", exc)
            return False

    def _fetch_ohlcv(self, symbol: str, start: date, end: date, interval: Interval) -> pd.DataFrame:
        if interval not in (Interval.ONE_DAY, Interval.ONE_WEEK):
            logger.debug("yfinance does not support intraday interval %s", interval)
            return pd.DataFrame()

        import yfinance as yf

        ticker = yf.Ticker(symbol)
        # yfinance end date is exclusive, so add 1 day
        from datetime import timedelta
        end_inclusive = end + timedelta(days=1)
        hist = ticker.history(
            start=start.isoformat(),
            end=end_inclusive.isoformat(),
            interval="1d",
            auto_adjust=False,
            actions=False,
        )

        if hist.empty:
            logger.warning("yfinance returned empty data for %s %s→%s", symbol, start, end)
            return pd.DataFrame()

        hist = hist.reset_index()
        # Normalize column names (yfinance uses Title Case)
        hist.columns = [c.lower().replace(" ", "_") for c in hist.columns]

        # Ensure timezone-aware timestamps
        if "date" in hist.columns:
            hist = hist.rename(columns={"date": "timestamp"})
        if hist["timestamp"].dt.tz is None:
            hist["timestamp"] = hist["timestamp"].dt.tz_localize("UTC")
        else:
            hist["timestamp"] = hist["timestamp"].dt.tz_convert("UTC")

        # Map yfinance columns to our schema
        col_map = {
            "adj_close": "adj_close",
            "adj close": "adj_close",
        }
        hist = hist.rename(columns=col_map)

        required = ["timestamp", "open", "high", "low", "close", "volume"]
        for col in required:
            if col not in hist.columns:
                logger.error("yfinance missing column %s for %s", col, symbol)
                return pd.DataFrame()

        if "adj_close" not in hist.columns:
            hist["adj_close"] = hist["close"]

        return self._enforce_ohlcv_schema(hist, symbol)
