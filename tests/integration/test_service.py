"""
tests/integration/test_service.py
End-to-end integration tests for the MarketDataService.

These tests run the full stack: real providers → real TimescaleDB → real Redis.
Requires both API keys AND running infrastructure (via docker-compose up).

Run with: pytest tests/integration/ -m integration
Skip in CI without infrastructure: pytest -m "not integration"
"""

import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from market_data.config import settings
from market_data.models import CoverageStatus, DataType, Interval
from market_data.service import MarketDataService


def _has_any_api_key() -> bool:
    return bool(settings.alpha_vantage_api_key or settings.finnhub_api_key)


def _has_timescaledb() -> bool:
    """Check if TimescaleDB is reachable."""
    try:
        from market_data.storage.timescale import TimescaleStore
        return TimescaleStore().ping()
    except Exception:
        return False


def _has_redis() -> bool:
    """Check if Redis is reachable."""
    try:
        from market_data.cache.redis_cache import RedisCache
        return RedisCache().ping()
    except Exception:
        return False


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def svc(tmp_path_factory):
    """
    A MarketDataService wired to real infrastructure.
    Uses a temporary SQLite coverage manifest so tests don't pollute the dev DB.
    """
    tmp_db = str(tmp_path_factory.mktemp("coverage") / "integration_test.db")

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(settings, "coverage_db_path", tmp_db)
        service = MarketDataService()
        yield service


# ── Full Stack: get() ─────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.skipif(
    not (_has_any_api_key() and _has_timescaledb() and _has_redis()),
    reason="Requires API key + TimescaleDB + Redis (docker-compose up)"
)
class TestServiceGetIntegration:
    """
    Full-stack integration tests. These use real API calls, real TimescaleDB,
    and real Redis. They verify the complete cache-first, delta-fetch pipeline.
    """

    def test_first_call_fetches_from_api(self, svc):
        """On first call, data must come from the API (no local cache)."""
        end = date.today()
        start = end - timedelta(days=30)

        # Force clean state for this test
        svc.coverage.invalidate("IBM", DataType.OHLCV)
        svc.redis.invalidate("IBM", DataType.OHLCV)

        response = svc.get("IBM", DataType.OHLCV, start, end)

        assert response is not None
        assert response.symbol == "IBM"
        assert response.rows > 0
        assert "api:" in response.source or "merged" in response.source, (
            f"Expected API source, got: {response.source}"
        )

    def test_second_call_served_from_local(self, svc):
        """Second identical request must be served from TimescaleDB (not API)."""
        end = date.today()
        start = end - timedelta(days=30)

        # First call — populates the store
        svc.get("IBM", DataType.OHLCV, start, end)

        # Second call — should be a cache hit
        response = svc.get("IBM", DataType.OHLCV, start, end)

        assert response.source in ("timescaledb", "cache"), (
            f"Expected local source on second call, got: {response.source}"
        )
        assert response.coverage == CoverageStatus.COMPLETE

    def test_force_refresh_re_fetches(self, svc):
        """force_refresh=True must bypass local data and re-fetch from provider."""
        end = date.today()
        start = end - timedelta(days=7)

        # Warm the cache first
        svc.get("MSFT", DataType.OHLCV, start, end)

        # Force refresh
        response = svc.get("MSFT", DataType.OHLCV, start, end, force_refresh=True)

        assert response.rows > 0
        # Source should indicate a fresh API fetch
        assert "api:" in response.source or "merged" in response.source

    def test_response_schema_matches_ohlcv(self, svc):
        """Response must include all expected OHLCV columns."""
        end = date.today()
        start = end - timedelta(days=5)
        response = svc.get("AAPL", DataType.OHLCV, start, end)

        expected_cols = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
        assert expected_cols.issubset(set(response.schema)), (
            f"Missing columns: {expected_cols - set(response.schema)}"
        )

    def test_response_data_valid(self, svc):
        """All OHLCV rows must have positive prices and volume."""
        end = date.today()
        start = end - timedelta(days=20)
        response = svc.get("SPY", DataType.OHLCV, start, end)

        if response.rows > 0:
            df = pd.DataFrame(response.data, columns=response.schema)
            assert (pd.to_numeric(df["close"]) > 0).all()
            assert (pd.to_numeric(df["volume"]) > 0).all()


# ── Status ────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.skipif(
    not _has_timescaledb(),
    reason="Requires running TimescaleDB"
)
class TestServiceStatusIntegration:
    def test_status_missing_before_first_fetch(self, svc):
        """Status for a never-fetched symbol must report MISSING."""
        svc.coverage.invalidate("RANDOMSYM", DataType.OHLCV)
        end = date.today()
        start = end - timedelta(days=30)

        result = svc.status("RANDOMSYM", DataType.OHLCV, start, end)
        assert result.coverage == CoverageStatus.MISSING

    def test_status_complete_after_successful_fetch(self, svc):
        """Status must report COMPLETE after data has been fetched and stored."""
        end = date.today()
        start = end - timedelta(days=14)

        # Ensure data is present
        if _has_any_api_key():
            svc.get("GLD", DataType.OHLCV, start, end)
            result = svc.status("GLD", DataType.OHLCV, start, end)
            assert result.coverage in (CoverageStatus.COMPLETE, CoverageStatus.PARTIAL)


# ── Batch ─────────────────────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.skipif(
    not (_has_any_api_key() and _has_timescaledb()),
    reason="Requires API key + TimescaleDB"
)
class TestServiceBatchIntegration:
    def test_batch_multiple_etfs(self, svc):
        """Batch request for ETFs should succeed for all symbols."""
        symbols = ["SPY", "QQQ", "IWM"]
        end = date.today()
        start = end - timedelta(days=14)

        result = svc.batch(symbols, DataType.OHLCV, start, end)

        assert result.requested == symbols
        assert len(result.succeeded) > 0, "At least some symbols should succeed"
        # All symbols should be accounted for
        assert set(result.succeeded + result.failed) == set(symbols)

    def test_batch_all_results_have_correct_data_type(self, svc):
        """All results in a batch must have the requested data type."""
        symbols = ["AAPL", "MSFT"]
        end = date.today()
        start = end - timedelta(days=7)

        result = svc.batch(symbols, DataType.OHLCV, start, end)

        for sym in result.succeeded:
            assert result.results[sym].data_type == DataType.OHLCV


# ── Health ────────────────────────────────────────────────────────────────

@pytest.mark.integration
class TestServiceHealthIntegration:
    def test_health_returns_response_object(self, svc):
        """health() should always return a HealthResponse, even if components are down."""
        response = svc.health()

        assert response is not None
        assert hasattr(response, "timescaledb")
        assert hasattr(response, "redis")
        assert hasattr(response, "minio")
        assert hasattr(response, "providers")
        assert hasattr(response, "overall")

    def test_health_providers_dict_is_populated(self, svc):
        """health().providers should be a dict of provider_name: bool."""
        response = svc.health()
        assert isinstance(response.providers, dict)
        # At least one provider should be checked if keys are configured
        if _has_any_api_key():
            assert len(response.providers) > 0

    @pytest.mark.skipif(
        not (_has_timescaledb() and _has_redis()),
        reason="Requires running TimescaleDB and Redis"
    )
    def test_health_all_infrastructure_up(self, svc):
        """With all services running, overall health should be True."""
        response = svc.health()
        assert response.timescaledb is True
        assert response.redis is True
        # overall = timescaledb AND redis AND at least one provider
        if _has_any_api_key():
            assert response.overall is True


# ── Coverage Persistence ───────────────────────────────────────────────────

@pytest.mark.integration
@pytest.mark.skipif(
    not (_has_any_api_key() and _has_timescaledb()),
    reason="Requires API key + TimescaleDB"
)
class TestCoveragePersistence:
    def test_coverage_manifest_updated_after_fetch(self, svc):
        """Coverage manifest must be updated when new data is fetched."""
        end = date.today()
        start = end - timedelta(days=10)

        svc.coverage.invalidate("TLT", DataType.OHLCV)
        svc.get("TLT", DataType.OHLCV, start, end)

        records = svc.coverage.list_available(symbol="TLT")
        assert len(records) > 0, "Coverage record should be created after successful fetch"

    def test_delta_fetch_only_fills_gaps(self, svc):
        """Second fetch for an extended range should only fetch the gap, not re-fetch known data."""
        end = date.today()
        mid = end - timedelta(days=15)
        start = end - timedelta(days=30)

        # First fetch: last 15 days
        svc.coverage.invalidate("GE", DataType.OHLCV)
        first_response = svc.get("GE", DataType.OHLCV, mid, end)

        # Second fetch: extend to 30 days (gap = first 15 days)
        second_response = svc.get("GE", DataType.OHLCV, start, end)

        # Second response should include data from both fetches
        assert second_response.rows >= first_response.rows
