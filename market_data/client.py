"""
market_data/client.py
Thin Python client wrapper — agents that import this get a clean DataFrame API
without needing to subprocess the CLI.
"""

from datetime import date, timedelta
from typing import Optional, Union

import pandas as pd

from market_data.models import DataType, Interval
from market_data.service import MarketDataService


class MarketDataClient:
    """
    Agent-facing Python client. Returns DataFrames directly.
    Use this when your agent is a Python process that can import the package.
    Use the CLI when spawning subprocess agents or working cross-language.

    Example:
        mds = MarketDataClient()
        df = mds.get("AAPL", "ohlcv", days=365)
        status = mds.status("AAPL", "ohlcv")
        mds.batch(["SPY", "QQQ"], "ohlcv", days=90)
    """

    def __init__(self):
        self._svc = MarketDataService()

    def get(
        self,
        symbol: str,
        data_type: Union[str, DataType] = "ohlcv",
        days: Optional[int] = 365,
        start: Optional[Union[str, date]] = None,
        end: Optional[Union[str, date]] = None,
        interval: Union[str, Interval] = "1d",
        force_refresh: bool = False,
        preferred_provider: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Fetch market data and return as a DataFrame.

        Args:
            symbol: Ticker symbol (e.g., "AAPL")
            data_type: DataType enum or string (e.g., "ohlcv")
            days: Number of days back from today (ignored if start/end provided)
            start: Start date (string or date object)
            end: End date (string or date object)
            interval: Bar interval ("1d", "1h", "5m", etc.)
            force_refresh: Bypass all caches and re-fetch from provider
            preferred_provider: Override provider ("alpha_vantage", "finnhub", "databento")

        Returns:
            pd.DataFrame with columns matching the data type schema
        """
        dt = DataType(data_type) if isinstance(data_type, str) else data_type
        iv = Interval(interval) if isinstance(interval, str) else interval

        end_date = self._resolve_date(end) or date.today()
        if start:
            start_date = self._resolve_date(start)
        else:
            start_date = end_date - timedelta(days=days or 365)

        response = self._svc.get(
            symbol=symbol,
            data_type=dt,
            start=start_date,
            end=end_date,
            interval=iv,
            force_refresh=force_refresh,
            preferred_provider=preferred_provider,
        )

        if not response.data:
            return pd.DataFrame(columns=response.schema)

        df = pd.DataFrame(response.data, columns=response.schema)
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df

    def status(
        self,
        symbol: str,
        data_type: Union[str, DataType] = "ohlcv",
        days: int = 365,
    ) -> dict:
        """Check local coverage status without fetching."""
        dt = DataType(data_type) if isinstance(data_type, str) else data_type
        end_date = date.today()
        start_date = end_date - timedelta(days=days)
        response = self._svc.status(symbol, dt, start_date, end_date)
        return response.model_dump()

    def batch(
        self,
        symbols: list[str],
        data_type: Union[str, DataType] = "ohlcv",
        days: int = 365,
        interval: Union[str, Interval] = "1d",
        max_workers: int = 4,
    ) -> dict[str, pd.DataFrame]:
        """
        Batch fetch for multiple symbols.
        Returns dict of {symbol: DataFrame}.
        """
        dt = DataType(data_type) if isinstance(data_type, str) else data_type
        iv = Interval(interval) if isinstance(interval, str) else interval
        end_date = date.today()
        start_date = end_date - timedelta(days=days)

        response = self._svc.batch(symbols, dt, start_date, end_date, iv, max_workers)

        result = {}
        for sym, data_response in response.results.items():
            if data_response.data:
                df = pd.DataFrame(data_response.data, columns=data_response.schema)
                if "timestamp" in df.columns:
                    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
                result[sym] = df
            else:
                result[sym] = pd.DataFrame(columns=data_response.schema)
        return result

    def warm(
        self,
        symbols: list[str],
        data_types: Optional[list[str]] = None,
        days: int = 365,
    ) -> dict:
        """Pre-populate local cache for a watchlist."""
        dts = [DataType(dt) for dt in (data_types or ["ohlcv"])]
        return self._svc.warm(symbols, dts, days)

    def health(self) -> dict:
        """Check infrastructure health."""
        return self._svc.health().model_dump()

    @staticmethod
    def _resolve_date(val: Optional[Union[str, date]]) -> Optional[date]:
        if val is None:
            return None
        if isinstance(val, date):
            return val
        return date.fromisoformat(val)
