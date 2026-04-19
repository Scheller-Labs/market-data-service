"""
tests/conftest.py
Shared pytest fixtures for the market data service test suite.

Fixtures here are available to all test modules without import.
Heavy infrastructure fixtures (TimescaleDB, Redis) are skipped unless
the corresponding environment variables are set.
"""

import json
import tempfile
from datetime import date, datetime, timezone, timedelta
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from market_data.cache.coverage import CoverageManifest
from market_data.models import (
    CoverageStatus, DataType, Interval,
    DataResponse, DateGap, CoverageReport,
)


# ── Coverage Manifest Fixtures ─────────────────────────────────────────────

@pytest.fixture
def tmp_db_path(tmp_path: Path) -> str:
    """Return a path to a fresh temporary SQLite database file."""
    return str(tmp_path / "coverage_test.db")


@pytest.fixture
def manifest(tmp_db_path: str) -> CoverageManifest:
    """Fresh CoverageManifest backed by a temporary SQLite file."""
    return CoverageManifest(db_path=tmp_db_path)


# ── Sample DataFrames ──────────────────────────────────────────────────────

@pytest.fixture
def sample_ohlcv_df() -> pd.DataFrame:
    """
    A realistic 5-row OHLCV DataFrame for AAPL using the canonical MDS schema.
    Timestamps are timezone-aware UTC, matching TimescaleDB and provider output.
    """
    base = datetime(2024, 1, 2, tzinfo=timezone.utc)
    rows = []
    prices = [(185.0, 188.5, 184.0, 187.5), (187.5, 190.0, 186.0, 189.2),
              (189.2, 191.0, 187.8, 188.0), (188.0, 189.5, 185.5, 186.5),
              (186.5, 187.0, 183.0, 185.8)]
    for i, (o, h, l, c) in enumerate(prices):
        rows.append({
            "timestamp": base + timedelta(days=i),
            "symbol":    "AAPL",
            "open":      o,
            "high":      h,
            "low":       l,
            "close":     c,
            "volume":    50_000_000 + i * 1_000_000,
            "adj_close": c,
            "provider":  "alpha_vantage",
        })
    return pd.DataFrame(rows)


@pytest.fixture
def sample_fundamentals_df() -> pd.DataFrame:
    """A minimal fundamentals row for AAPL."""
    return pd.DataFrame([{
        "snapshot_date": date(2024, 1, 2),
        "symbol":        "AAPL",
        "pe_ratio":      29.5,
        "eps":           6.13,
        "revenue":       394_330_000_000,
        "market_cap":    2_950_000_000_000,
        "debt_to_equity": 1.73,
        "roe":           1.56,
        "sector":        "Technology",
        "industry":      "Consumer Electronics",
        "raw_data":      {},
        "provider":      "alpha_vantage",
    }])


@pytest.fixture
def sample_earnings_df() -> pd.DataFrame:
    """A minimal earnings rows DataFrame for AAPL."""
    return pd.DataFrame([
        {
            "report_date":    date(2024, 1, 31),
            "symbol":         "AAPL",
            "eps_actual":     2.18,
            "eps_estimate":   2.10,
            "eps_surprise":   0.08,
            "revenue_actual": 119_575_000_000,
            "revenue_estimate": 117_900_000_000,
            "fiscal_quarter": "Q1 2024",
            "fiscal_year":    2024,
            "provider":       "alpha_vantage",
        },
        {
            "report_date":    date(2023, 10, 26),
            "symbol":         "AAPL",
            "eps_actual":     1.46,
            "eps_estimate":   1.39,
            "eps_surprise":   0.07,
            "revenue_actual": 89_498_000_000,
            "revenue_estimate": 89_300_000_000,
            "fiscal_quarter": "Q4 2023",
            "fiscal_year":    2023,
            "provider":       "alpha_vantage",
        },
    ])


@pytest.fixture
def sample_news_df() -> pd.DataFrame:
    """A minimal news sentiment DataFrame for AAPL."""
    return pd.DataFrame([
        {
            "published_at":    datetime(2024, 1, 10, 9, 0, tzinfo=timezone.utc),
            "symbol":          "AAPL",
            "headline":        "Apple Reports Record Q1 Earnings",
            "source":          "Reuters",
            "sentiment_score": 0.72,
            "sentiment_label": "positive",
            "url":             "https://example.com/article1",
            "provider":        "finnhub",
        },
        {
            "published_at":    datetime(2024, 1, 11, 14, 0, tzinfo=timezone.utc),
            "symbol":          "AAPL",
            "headline":        "Apple Vision Pro Sales Disappoint",
            "source":          "Bloomberg",
            "sentiment_score": -0.31,
            "sentiment_label": "negative",
            "url":             "https://example.com/article2",
            "provider":        "finnhub",
        },
    ])


# ── Mock Provider Fixtures ─────────────────────────────────────────────────

@pytest.fixture
def mock_alpha_vantage_provider(sample_ohlcv_df, sample_fundamentals_df, sample_earnings_df):
    """
    A fully mocked AlphaVantageProvider.
    All fetch methods return sample DataFrames instead of calling the API.
    """
    provider = MagicMock()
    provider.name = "alpha_vantage"
    provider.supports = lambda dt: dt in [
        DataType.OHLCV, DataType.OHLCV_INTRADAY,
        DataType.FUNDAMENTALS, DataType.EARNINGS,
        DataType.NEWS_SENTIMENT, DataType.DIVIDENDS,
    ]
    provider.fetch_ohlcv.return_value = sample_ohlcv_df
    provider.fetch_fundamentals.return_value = sample_fundamentals_df
    provider.fetch_earnings.return_value = sample_earnings_df
    provider.fetch_news_sentiment.return_value = pd.DataFrame()
    provider.fetch_dividends.return_value = pd.DataFrame()
    provider.health_check.return_value = True
    return provider


@pytest.fixture
def mock_finnhub_provider(sample_ohlcv_df):
    """A fully mocked FinnhubProvider."""
    provider = MagicMock()
    provider.name = "finnhub"
    provider.supports = lambda dt: dt in [
        DataType.OHLCV, DataType.OHLCV_INTRADAY,
        DataType.OPTIONS_CHAIN, DataType.NEWS_SENTIMENT,
        DataType.EARNINGS, DataType.FUNDAMENTALS, DataType.IV_RANK,
    ]
    provider.fetch_ohlcv.return_value = sample_ohlcv_df
    provider.fetch_options_chain.return_value = pd.DataFrame()
    provider.fetch_news_sentiment.return_value = pd.DataFrame()
    provider.fetch_earnings.return_value = pd.DataFrame()
    provider.health_check.return_value = True
    return provider


# ── Mock Infrastructure Fixtures ──────────────────────────────────────────

@pytest.fixture
def mock_redis_cache():
    """A mocked RedisCache that never touches real Redis."""
    cache = MagicMock()
    cache.get.return_value = None    # always miss
    cache.set.return_value = True
    cache.ping.return_value = True
    cache.invalidate.return_value = 0
    return cache


@pytest.fixture
def mock_timescale_store(sample_ohlcv_df):
    """
    A mocked TimescaleStore that returns sample data without hitting a DB.
    The query method returns sample_ohlcv_df by default.
    """
    store = MagicMock()
    store.query.return_value = sample_ohlcv_df
    store.query_ohlcv.return_value = sample_ohlcv_df
    store.upsert_ohlcv.return_value = 5
    store.upsert_fundamentals.return_value = 1
    store.upsert_earnings.return_value = 2
    store.upsert_dividends.return_value = 0
    store.upsert_news_sentiment.return_value = 0
    store.upsert_options_snapshot.return_value = 0
    store.ping.return_value = True
    return store


@pytest.fixture
def mock_provider_router(mock_alpha_vantage_provider, mock_finnhub_provider):
    """A mocked ProviderRouter that returns mock providers."""
    router = MagicMock()
    router.select.return_value = mock_alpha_vantage_provider
    router.health_check_all.return_value = {
        "alpha_vantage": True,
        "finnhub": True,
    }
    return router


# ── Data Response Helpers ──────────────────────────────────────────────────

@pytest.fixture
def sample_data_response(sample_ohlcv_df) -> DataResponse:
    """A complete, valid DataResponse object with OHLCV data."""
    rows = sample_ohlcv_df.to_dict("records")
    # Serialize datetimes
    for row in rows:
        for k, v in row.items():
            if isinstance(v, datetime):
                row[k] = v.isoformat()

    return DataResponse(
        symbol="AAPL",
        data_type=DataType.OHLCV,
        interval="1d",
        source="timescaledb",
        coverage=CoverageStatus.COMPLETE,
        gaps=[],
        rows=len(rows),
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        schema=list(sample_ohlcv_df.columns),
        data=rows,
    )


# ── Date Range Helpers ─────────────────────────────────────────────────────

@pytest.fixture
def date_range_1yr():
    """Standard 1-year date range ending today."""
    end = date.today()
    start = end - timedelta(days=365)
    return start, end


@pytest.fixture
def date_range_30d():
    """Standard 30-day date range ending today."""
    end = date.today()
    start = end - timedelta(days=30)
    return start, end
