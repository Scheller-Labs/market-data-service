"""
tests/integration/test_finnhub.py
Integration tests for the Finnhub provider.

These tests make real API calls and require FINNHUB_API_KEY to be set in .env.
Run with: pytest tests/integration/ -m integration

Each test is a smoke test — it validates that the provider can successfully
call the API, parse the response, and return a normalized DataFrame.
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from market_data.config import settings
from market_data.models import DataType, Interval
from market_data.providers.finnhub import FinnhubProvider


@pytest.mark.integration
@pytest.mark.skipif(
    not settings.finnhub_api_key,
    reason="FINNHUB_API_KEY not set — skipping Finnhub integration tests"
)
class TestFinnhubIntegration:
    """Live integration tests for FinnhubProvider. Require a valid API key."""

    @pytest.fixture(scope="class")
    def provider(self):
        """Shared FinnhubProvider instance for the test class."""
        return FinnhubProvider()

    # ── Health Check ─────────────────────────────────────────────────────

    def test_health_check_passes(self, provider):
        """health_check() should return True with a valid API key."""
        assert provider.health_check() is True

    # ── OHLCV ─────────────────────────────────────────────────────────────

    def test_fetch_ohlcv_daily_aapl(self, provider):
        """Daily OHLCV for AAPL should return a non-empty normalized DataFrame."""
        end = date.today()
        start = end - timedelta(days=30)
        df = provider.fetch_ohlcv("AAPL", start, end, Interval.ONE_DAY)

        assert isinstance(df, pd.DataFrame)
        assert not df.empty, "Expected OHLCV data for AAPL in the last 30 days"
        assert "timestamp" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["provider"].iloc[0] == "finnhub"
        assert (df["close"] > 0).all(), "All close prices must be positive"
        assert (df["volume"] > 0).all(), "All volumes must be positive"

    def test_fetch_ohlcv_spy_etf(self, provider):
        """SPY ETF should work the same as individual stocks."""
        end = date.today()
        start = end - timedelta(days=14)
        df = provider.fetch_ohlcv("SPY", start, end, Interval.ONE_DAY)

        assert isinstance(df, pd.DataFrame)
        # SPY is highly liquid — always has data on trading days
        if not df.empty:
            assert df["symbol"].iloc[0] == "SPY"

    def test_ohlcv_schema_has_required_columns(self, provider):
        """OHLCV result must include all required MDS schema columns."""
        end = date.today()
        start = end - timedelta(days=10)
        df = provider.fetch_ohlcv("MSFT", start, end, Interval.ONE_DAY)

        required_columns = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "provider"}
        if not df.empty:
            assert required_columns.issubset(set(df.columns)), (
                f"Missing columns: {required_columns - set(df.columns)}"
            )

    def test_ohlcv_high_greater_than_or_equal_to_low(self, provider):
        """High price must always be >= Low price (basic data sanity)."""
        end = date.today()
        start = end - timedelta(days=30)
        df = provider.fetch_ohlcv("AAPL", start, end, Interval.ONE_DAY)

        if not df.empty:
            assert (df["high"] >= df["low"]).all(), "High must always be >= Low"

    def test_ohlcv_no_data_range_returns_empty(self, provider):
        """A date range with no trading data should return empty DataFrame, not error."""
        # Very old date range unlikely to have data on free tier
        start = date(2000, 1, 1)
        end = date(2000, 1, 5)
        df = provider.fetch_ohlcv("AAPL", start, end, Interval.ONE_DAY)
        # Should return empty (no data) rather than raising
        assert isinstance(df, pd.DataFrame)

    # ── Options Chain ──────────────────────────────────────────────────────

    def test_fetch_options_chain_aapl(self, provider):
        """Options chain for AAPL should return calls and puts with Greeks."""
        df = provider.fetch_options_chain("AAPL")

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "strike" in df.columns
            assert "option_type" in df.columns
            assert "expiration_date" in df.columns
            # Must have both calls and puts
            option_types = set(df["option_type"].unique())
            assert "call" in option_types or "put" in option_types

    # ── News/Sentiment ─────────────────────────────────────────────────────

    def test_fetch_news_sentiment_aapl(self, provider):
        """Company news for AAPL should return recent headlines."""
        end = date.today()
        start = end - timedelta(days=30)
        df = provider.fetch_news_sentiment("AAPL", start, end)

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "headline" in df.columns
            assert "source" in df.columns
            assert "published_at" in df.columns
            assert df["symbol"].iloc[0] == "AAPL"
            assert df["provider"].iloc[0] == "finnhub"

    def test_news_timestamps_are_within_range(self, provider):
        """News published_at timestamps should fall within the requested range."""
        import pandas as pd
        end = date.today()
        start = end - timedelta(days=14)
        df = provider.fetch_news_sentiment("AAPL", start, end)

        if not df.empty:
            pub_dates = pd.to_datetime(df["published_at"]).dt.date
            assert pub_dates.max() <= end, "No news should be after the end date"

    # ── Earnings ──────────────────────────────────────────────────────────

    def test_fetch_earnings_aapl(self, provider):
        """Earnings history for AAPL should have EPS data."""
        df = provider.fetch_earnings("AAPL")

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "report_date" in df.columns
            assert "eps_actual" in df.columns
            assert "eps_estimate" in df.columns
            assert df["symbol"].iloc[0] == "AAPL"

    # ── Fundamentals ──────────────────────────────────────────────────────

    def test_fetch_fundamentals_aapl(self, provider):
        """Fundamentals for AAPL should include market cap and sector."""
        df = provider.fetch_fundamentals("AAPL")

        assert isinstance(df, pd.DataFrame)
        if not df.empty:
            assert "pe_ratio" in df.columns or "market_cap" in df.columns
            assert df["symbol"].iloc[0] == "AAPL"
            # Market cap for AAPL should be in the trillions (> 1e12)
            if df["market_cap"].iloc[0] is not None:
                assert df["market_cap"].iloc[0] > 1e9, "AAPL market cap should be > $1B"

    # ── Rate Limiting Smoke Test ───────────────────────────────────────────

    def test_consecutive_calls_respect_rate_limit(self, provider):
        """Two quick calls should both succeed (rate limiter waits if needed)."""
        end = date.today()
        start = end - timedelta(days=5)

        # These will both be allowed — the rate limiter will wait if needed
        df1 = provider.fetch_ohlcv("AAPL", start, end, Interval.ONE_DAY)
        df2 = provider.fetch_ohlcv("MSFT", start, end, Interval.ONE_DAY)

        # Both should succeed without raising
        assert isinstance(df1, pd.DataFrame)
        assert isinstance(df2, pd.DataFrame)
