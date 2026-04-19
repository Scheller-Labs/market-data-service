"""
market_data/providers/base.py
Abstract base class for all market data providers.
Every provider adapter implements this interface.
"""

import time
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Optional
from dataclasses import dataclass, field

import pandas as pd

from market_data.models import DataType, Interval, OHLCVRow

logger = logging.getLogger(__name__)


@dataclass
class RateLimitConfig:
    calls_per_minute: Optional[int] = None
    calls_per_day: Optional[int] = None
    min_interval_seconds: float = 0.0   # minimum seconds between calls


class RateLimitExceeded(Exception):
    """Raised when a provider's rate limit would be exceeded."""
    def __init__(self, provider: str, reset_in_seconds: float):
        self.provider = provider
        self.reset_in_seconds = reset_in_seconds
        super().__init__(
            f"Rate limit exceeded for {provider}. "
            f"Next call allowed in {reset_in_seconds:.1f}s."
        )


class TokenBucketRateLimiter:
    """
    Token bucket rate limiter.
    Thread-safe for single-process use; use Redis for multi-process.
    """

    def __init__(self, config: RateLimitConfig, provider_name: str):
        self.config = config
        self.provider_name = provider_name
        self._last_call_time: float = 0.0
        self._minute_calls: list[float] = []   # timestamps of calls in last 60s

    def check_and_record(self) -> None:
        """
        Check if a call is allowed. If not, raise RateLimitExceeded.
        If yes, record the call timestamp.
        """
        now = time.monotonic()

        # Enforce minimum interval between calls
        if self.config.min_interval_seconds > 0:
            elapsed = now - self._last_call_time
            if elapsed < self.config.min_interval_seconds:
                wait = self.config.min_interval_seconds - elapsed
                raise RateLimitExceeded(self.provider_name, wait)

        # Enforce calls-per-minute window
        if self.config.calls_per_minute:
            cutoff = now - 60.0
            self._minute_calls = [t for t in self._minute_calls if t > cutoff]
            if len(self._minute_calls) >= self.config.calls_per_minute:
                oldest = self._minute_calls[0]
                wait = 60.0 - (now - oldest)
                raise RateLimitExceeded(self.provider_name, max(0.1, wait))
            self._minute_calls.append(now)

        self._last_call_time = now

    def wait_if_needed(self) -> None:
        """Like check_and_record but sleeps instead of raising."""
        while True:
            try:
                self.check_and_record()
                return
            except RateLimitExceeded as e:
                logger.debug(f"Rate limiting {self.provider_name}: sleeping {e.reset_in_seconds:.2f}s")
                time.sleep(e.reset_in_seconds + 0.05)


class BaseProvider(ABC):
    """
    Abstract base for all market data providers.
    Subclasses implement _fetch_* methods with their API-specific logic.
    The base class handles rate limiting, retries, and logging.
    """

    name: str = "base"

    def __init__(self):
        rate_config = self.get_rate_limit_config()
        self._limiter = TokenBucketRateLimiter(rate_config, self.name)

    @abstractmethod
    def get_rate_limit_config(self) -> RateLimitConfig:
        """Return this provider's rate limit configuration."""
        ...

    @abstractmethod
    def supported_data_types(self) -> list[DataType]:
        """Data types this provider can supply."""
        ...

    def supports(self, data_type: DataType) -> bool:
        return data_type in self.supported_data_types()

    # ── Public Methods (rate-limit enforced) ───────────────────────────────

    def fetch_ohlcv(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: Interval = Interval.ONE_DAY,
    ) -> pd.DataFrame:
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching OHLCV {symbol} {start}→{end} ({interval})")
        return self._fetch_ohlcv(symbol, start, end, interval)

    def fetch_fundamentals(self, symbol: str) -> pd.DataFrame:
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching fundamentals {symbol}")
        return self._fetch_fundamentals(symbol)

    def fetch_options_chain(self, symbol: str, snapshot_date: Optional[date] = None) -> pd.DataFrame:
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching options chain {symbol}")
        return self._fetch_options_chain(symbol, snapshot_date)

    def fetch_news_sentiment(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching news {symbol} {start}→{end}")
        return self._fetch_news_sentiment(symbol, start, end)

    def fetch_earnings(self, symbol: str) -> pd.DataFrame:
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching earnings {symbol}")
        return self._fetch_earnings(symbol)

    def fetch_dividends(self, symbol: str) -> pd.DataFrame:
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching dividends {symbol}")
        return self._fetch_dividends(symbol)

    def fetch_iv_rank(self, symbol: str) -> pd.DataFrame:
        """
        Fetch the current raw ATM implied volatility for a symbol.

        Returns a single-row DataFrame with columns:
          recorded_at, symbol, current_iv, provider

        The caller (MarketDataService.get_iv_rank) is responsible for
        computing iv_rank and iv_percentile from stored history.
        """
        self._limiter.wait_if_needed()
        logger.info(f"[{self.name}] Fetching IV (raw ATM) for {symbol}")
        return self._fetch_iv_rank(symbol)

    def health_check(self) -> bool:
        """Ping the provider API to verify connectivity and auth."""
        try:
            return self._health_check()
        except Exception as e:
            logger.warning(f"[{self.name}] Health check failed: {e}")
            return False

    # ── Abstract Private Methods (providers implement these) ───────────────

    @abstractmethod
    def _fetch_ohlcv(self, symbol: str, start: date, end: date, interval: Interval) -> pd.DataFrame:
        ...

    def _fetch_fundamentals(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support fundamentals")

    def _fetch_options_chain(self, symbol: str, snapshot_date: Optional[date]) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support options chain")

    def _fetch_news_sentiment(self, symbol: str, start: date, end: date) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support news sentiment")

    def _fetch_earnings(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support earnings")

    def _fetch_dividends(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support dividends")

    def _fetch_iv_rank(self, symbol: str) -> pd.DataFrame:
        raise NotImplementedError(f"{self.name} does not support IV rank")

    @abstractmethod
    def _health_check(self) -> bool:
        ...

    # ── DataFrame Schema Enforcement ──────────────────────────────────────

    OHLCV_COLUMNS = ["timestamp", "symbol", "open", "high", "low", "close", "volume", "adj_close", "provider"]

    def _enforce_ohlcv_schema(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Ensure DataFrame has the canonical OHLCV schema."""
        df = df.copy()
        df["symbol"] = symbol.upper()
        df["provider"] = self.name
        if "adj_close" not in df.columns:
            df["adj_close"] = None

        # Ensure timestamp is timezone-aware UTC
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)

        # Type coercion
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0).astype(int)

        # Keep only expected columns, in order
        return df[[c for c in self.OHLCV_COLUMNS if c in df.columns]]
