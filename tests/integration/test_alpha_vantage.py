"""
tests/integration/test_alpha_vantage.py
Integration tests for the Alpha Vantage provider.
These tests make real API calls — requires ALPHA_VANTAGE_API_KEY in .env
Run with: pytest tests/integration/ -m integration
"""

from datetime import date, timedelta

import pandas as pd
import pytest

from market_data.config import settings
from market_data.models import DataType, Interval
from market_data.providers.alpha_vantage import AlphaVantageProvider


@pytest.mark.integration
@pytest.mark.skipif(
    not settings.alpha_vantage_api_key,
    reason="ALPHA_VANTAGE_API_KEY not set"
)
class TestAlphaVantageIntegration:

    @pytest.fixture
    def provider(self):
        return AlphaVantageProvider()

    def test_health_check(self, provider):
        assert provider.health_check() is True

    def test_fetch_ohlcv_daily(self, provider):
        end = date.today()
        start = end - timedelta(days=30)
        df = provider.fetch_ohlcv("AAPL", start, end, Interval.ONE_DAY)

        assert not df.empty
        assert "timestamp" in df.columns
        assert "close" in df.columns
        assert "volume" in df.columns
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["provider"].iloc[0] == "alpha_vantage"
        assert (df["close"] > 0).all()

    def test_fetch_fundamentals(self, provider):
        df = provider.fetch_fundamentals("AAPL")
        assert not df.empty
        assert "pe_ratio" in df.columns
        assert "market_cap" in df.columns
        assert df["symbol"].iloc[0] == "AAPL"

    def test_fetch_earnings(self, provider):
        df = provider.fetch_earnings("AAPL")
        assert not df.empty
        assert "eps_actual" in df.columns
        assert "report_date" in df.columns

    def test_ohlcv_date_range_filter(self, provider):
        """Returned data should be within the requested date range."""
        end = date.today()
        start = end - timedelta(days=10)
        df = provider.fetch_ohlcv("AAPL", start, end, Interval.ONE_DAY)

        if not df.empty:
            dates = pd.to_datetime(df["timestamp"]).dt.date
            assert dates.min() >= start
            assert dates.max() <= end

    def test_schema_normalized(self, provider):
        """All expected columns should be present after normalization."""
        end = date.today()
        start = end - timedelta(days=5)
        df = provider.fetch_ohlcv("IBM", start, end, Interval.ONE_DAY)

        required_columns = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "provider"}
        assert required_columns.issubset(set(df.columns))
