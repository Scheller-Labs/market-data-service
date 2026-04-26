"""
tests/unit/test_service.py
Unit tests for the MarketDataService orchestration layer.

All external dependencies (TimescaleDB, Redis, providers) are mocked.
These tests verify the get/status/batch/warm/health logic in isolation.
"""

from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from market_data.models import (
    CoverageStatus, DataType, Interval,
    CoverageReport, DateGap, DataResponse,
)
from market_data.service import MarketDataService


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def svc(
    manifest,
    mock_redis_cache,
    mock_timescale_store,
    mock_provider_router,
):
    """
    A MarketDataService with all infrastructure replaced by mocks.
    Uses the tmp-file-backed CoverageManifest so gap detection logic runs for real.
    """
    service = MarketDataService.__new__(MarketDataService)
    service.coverage = manifest
    service.redis = mock_redis_cache
    service.store = mock_timescale_store
    service.router = mock_provider_router
    return service


@pytest.fixture
def svc_with_complete_coverage(svc, date_range_1yr):
    """Service whose coverage manifest already reports the full range as complete."""
    start, end = date_range_1yr
    svc.coverage.record("AAPL", DataType.OHLCV, start, end, "alpha_vantage", Interval.ONE_DAY, row_count=252)
    return svc


# ── get() — Coverage Paths ────────────────────────────────────────────────

class TestServiceGet:
    def test_get_complete_coverage_serves_from_store(self, svc_with_complete_coverage, date_range_1yr, sample_ohlcv_df):
        """When coverage is complete, data is served from TimescaleDB without API calls."""
        start, end = date_range_1yr
        svc = svc_with_complete_coverage

        response = svc.get("AAPL", DataType.OHLCV, start, end)

        assert response.symbol == "AAPL"
        assert response.source == "timescaledb"
        assert response.coverage == CoverageStatus.COMPLETE
        assert response.gaps == []
        # Verify no provider call was made
        svc.router.select.assert_not_called()

    def test_get_no_coverage_fetches_from_provider(self, svc, date_range_30d, mock_alpha_vantage_provider, sample_ohlcv_df):
        """When there is no local coverage, a provider call must be made."""
        start, end = date_range_30d
        svc.router.select.return_value = mock_alpha_vantage_provider

        response = svc.get("AAPL", DataType.OHLCV, start, end)

        svc.router.select.assert_called_once()
        mock_alpha_vantage_provider.fetch_ohlcv.assert_called_once()
        assert response.symbol == "AAPL"

    def test_get_force_refresh_bypasses_coverage(self, svc_with_complete_coverage, date_range_30d, mock_alpha_vantage_provider):
        """force_refresh=True must invalidate coverage and re-fetch from provider."""
        start, end = date_range_30d
        svc = svc_with_complete_coverage
        svc.router.select.return_value = mock_alpha_vantage_provider

        svc.get("AAPL", DataType.OHLCV, start, end, force_refresh=True)

        # Provider should have been called despite complete coverage
        svc.router.select.assert_called()

    def test_get_symbol_normalized_to_uppercase(self, svc, date_range_30d, mock_alpha_vantage_provider):
        """Symbols should be uppercased regardless of input casing."""
        start, end = date_range_30d
        svc.router.select.return_value = mock_alpha_vantage_provider

        response = svc.get("aapl", DataType.OHLCV, start, end)

        assert response.symbol == "AAPL"

    def test_get_empty_provider_response_returns_empty_df(self, svc, date_range_30d, mock_alpha_vantage_provider):
        """Empty DataFrame from provider should produce a response with 0 rows."""
        start, end = date_range_30d
        mock_alpha_vantage_provider.fetch_ohlcv.return_value = pd.DataFrame()
        svc.router.select.return_value = mock_alpha_vantage_provider
        svc.store.query.return_value = pd.DataFrame()

        response = svc.get("AAPL", DataType.OHLCV, start, end)

        assert response.rows == 0
        assert response.data == []

    def test_get_dry_run_skips_provider(self, svc, date_range_30d):
        """In dry_run mode, gaps are identified but no provider is called."""
        start, end = date_range_30d

        with patch("market_data.service.settings") as mock_settings:
            mock_settings.dry_run = True
            mock_settings.max_batch_workers = 4
            svc.get("AAPL", DataType.OHLCV, start, end)

        svc.router.select.assert_not_called()

    def test_get_persists_new_data(self, svc, date_range_30d, mock_alpha_vantage_provider, sample_ohlcv_df):
        """After fetching from provider, data must be written to TimescaleDB."""
        start, end = date_range_30d
        mock_alpha_vantage_provider.fetch_ohlcv.return_value = sample_ohlcv_df
        svc.router.select.return_value = mock_alpha_vantage_provider

        svc.get("AAPL", DataType.OHLCV, start, end)

        svc.store.upsert_ohlcv.assert_called_once()

    def test_get_provider_error_is_logged_not_raised(self, svc, date_range_30d, mock_alpha_vantage_provider):
        """Provider errors during gap fetch should be logged but not crash the service."""
        start, end = date_range_30d
        mock_alpha_vantage_provider.fetch_ohlcv.side_effect = RuntimeError("API timeout")
        svc.router.select.return_value = mock_alpha_vantage_provider
        svc.store.query.return_value = pd.DataFrame()

        # Should not raise
        response = svc.get("AAPL", DataType.OHLCV, start, end)
        assert response is not None


# ── status() ──────────────────────────────────────────────────────────────

class TestServiceStatus:
    def test_status_missing_when_no_records(self, svc, date_range_1yr):
        """status() on a fresh manifest should report MISSING."""
        start, end = date_range_1yr
        result = svc.status("AAPL", DataType.OHLCV, start, end)

        assert result.symbol == "AAPL"
        assert result.coverage == CoverageStatus.MISSING
        assert len(result.gaps) == 1

    def test_status_complete_after_recording(self, svc, date_range_1yr):
        """status() should report COMPLETE after the range is recorded."""
        start, end = date_range_1yr
        svc.coverage.record("AAPL", DataType.OHLCV, start, end, "alpha_vantage")

        result = svc.status("AAPL", DataType.OHLCV, start, end)

        assert result.coverage == CoverageStatus.COMPLETE
        assert result.gaps == []

    def test_status_does_not_call_provider(self, svc, date_range_1yr):
        """status() must never trigger a provider fetch."""
        start, end = date_range_1yr
        svc.status("AAPL", DataType.OHLCV, start, end)
        svc.router.select.assert_not_called()

    def test_status_returns_available_ranges(self, svc, date_range_1yr):
        """status() should include the covered ranges in its response."""
        start, end = date_range_1yr
        mid = start + timedelta(days=180)
        svc.coverage.record("AAPL", DataType.OHLCV, start, mid, "alpha_vantage")

        result = svc.status("AAPL", DataType.OHLCV, start, end)

        assert len(result.available_ranges) > 0


# ── batch() ───────────────────────────────────────────────────────────────

class TestServiceBatch:
    def test_batch_returns_results_for_all_symbols(self, svc, date_range_30d):
        """batch() must return a BatchResponse with entries for every symbol."""
        start, end = date_range_30d
        symbols = ["AAPL", "TSLA", "SPY"]
        svc.store.query.return_value = pd.DataFrame()

        result = svc.batch(symbols, DataType.OHLCV, start, end)

        assert set(result.requested) == set(symbols)
        assert set(result.succeeded + result.failed) == set(symbols)

    def test_batch_handles_partial_failures(self, svc, date_range_30d, sample_data_response):
        """Failed symbols should be in the 'failed' list, not crash the batch."""
        start, end = date_range_30d

        def get_side_effect(sym, *args, **kwargs):
            if sym == "BADTICKER":
                raise RuntimeError("Symbol not found")
            return sample_data_response

        with patch.object(svc, "get", side_effect=get_side_effect):
            result = svc.batch(["AAPL", "BADTICKER"], DataType.OHLCV, start, end)

        assert "BADTICKER" in result.failed
        assert "AAPL" in result.succeeded

    def test_batch_uses_thread_pool(self, svc, date_range_30d):
        """batch() should use ThreadPoolExecutor, not sequential fetches."""
        start, end = date_range_30d
        symbols = ["AAPL", "TSLA"]
        svc.store.query.return_value = pd.DataFrame()

        with patch("market_data.service.ThreadPoolExecutor", wraps=__import__("concurrent.futures", fromlist=["ThreadPoolExecutor"]).ThreadPoolExecutor) as mock_executor:
            svc.batch(symbols, DataType.OHLCV, start, end)
            mock_executor.assert_called_once()


# ── warm() ────────────────────────────────────────────────────────────────

class TestServiceWarm:
    def test_warm_calls_batch_for_each_type(self, svc):
        """warm() should fetch each data type for all symbols."""
        with patch.object(svc, "batch") as mock_batch:
            mock_batch.return_value = MagicMock(succeeded=["AAPL"], failed=[])
            result = svc.warm(["AAPL"], [DataType.OHLCV, DataType.FUNDAMENTALS], days=90)

        assert mock_batch.call_count == 2
        assert "ohlcv" in result["results"]
        assert "fundamentals" in result["results"]

    def test_warm_result_structure(self, svc):
        """warm() result should contain symbols, data_types, and per-type results."""
        with patch.object(svc, "batch") as mock_batch:
            mock_batch.return_value = MagicMock(succeeded=["AAPL"], failed=[])
            result = svc.warm(["AAPL"], [DataType.OHLCV])

        assert result["symbols"] == ["AAPL"]
        assert "ohlcv" in result["data_types"]


# ── health() ──────────────────────────────────────────────────────────────

class TestServiceHealth:
    def test_health_all_ok(self, svc):
        """health() should report overall=True when all components are up."""
        with patch("market_data.service.settings") as mock_settings:
            mock_settings.minio_endpoint = "localhost:9000"
            mock_settings.minio_access_key = "mds"
            mock_settings.minio_secret_key = "mds_secret"
            mock_settings.minio_secure = False
            with patch("market_data.service.Minio") as mock_minio_cls:
                mock_minio_cls.return_value.list_buckets.return_value = []
                response = svc.health()

        assert response.timescaledb is True
        assert response.redis is True
        assert response.overall is True

    def test_health_timescale_down(self, svc):
        """health() with TimescaleDB down should still return a response."""
        svc.store.ping.return_value = False
        with patch("market_data.service.settings") as mock_settings:
            mock_settings.minio_endpoint = "localhost:9000"
            mock_settings.minio_access_key = "mds"
            mock_settings.minio_secret_key = "mds_secret"
            mock_settings.minio_secure = False
            with patch("market_data.service.Minio") as mock_minio_cls:
                mock_minio_cls.return_value.list_buckets.side_effect = Exception("conn refused")
                response = svc.health()

        assert response.timescaledb is False

    def test_health_minio_down_does_not_crash(self, svc):
        """MinIO being down should set minio=False but not raise."""
        with patch("market_data.service.settings") as mock_settings:
            mock_settings.minio_endpoint = "localhost:9000"
            mock_settings.minio_access_key = "mds"
            mock_settings.minio_secret_key = "mds_secret"
            mock_settings.minio_secure = False
            with patch("market_data.service.Minio") as mock_minio_cls:
                mock_minio_cls.return_value.list_buckets.side_effect = Exception("timeout")
                response = svc.health()

        assert response.minio is False
        assert response is not None


# ── _build_response() ─────────────────────────────────────────────────────

class TestBuildResponse:
    def test_build_response_from_dataframe(self, svc, sample_ohlcv_df):
        """_build_response should correctly serialize a DataFrame to DataResponse."""
        response = svc._build_response(
            sample_ohlcv_df, "AAPL", DataType.OHLCV,
            Interval.ONE_DAY, "timescaledb", CoverageStatus.COMPLETE, []
        )

        assert response.symbol == "AAPL"
        assert response.rows == len(sample_ohlcv_df)
        assert response.source == "timescaledb"
        assert response.schema == list(sample_ohlcv_df.columns)
        assert len(response.data) == len(sample_ohlcv_df)

    def test_build_response_empty_df(self, svc):
        """Empty DataFrame should produce rows=0 and empty data list."""
        response = svc._build_response(
            pd.DataFrame(), "AAPL", DataType.OHLCV,
            Interval.ONE_DAY, "cache", CoverageStatus.MISSING, []
        )
        assert response.rows == 0
        assert response.data == []
        assert response.schema == []

    def test_build_response_serializes_datetimes(self, svc, sample_ohlcv_df):
        """datetime objects in data rows must be serialized to ISO strings."""
        response = svc._build_response(
            sample_ohlcv_df, "AAPL", DataType.OHLCV,
            Interval.ONE_DAY, "timescaledb", CoverageStatus.COMPLETE, []
        )
        for row in response.data:
            if "timestamp" in row:
                assert isinstance(row["timestamp"], str), \
                    "timestamp must be a string (ISO format) in response data"


# ── MarketDataService.get_iv_rank ──────────────────────────────────────────

class TestGetIVRank:
    """Tests for MarketDataService.get_iv_rank()."""

    @pytest.fixture
    def svc(self):
        with patch("market_data.service.CoverageManifest"), \
             patch("market_data.service.RedisCache"), \
             patch("market_data.service.TimescaleStore"), \
             patch("market_data.service.ProviderRouter"):
            return MarketDataService()

    def _make_history_df(self, ivs: list[float], today: date) -> pd.DataFrame:
        rows = []
        for i, iv in enumerate(ivs):
            d = today - timedelta(days=len(ivs) - i)
            rows.append({
                "recorded_at": d, "symbol": "SPY",
                "iv_rank": 50.0, "iv_percentile": 50.0,
                "current_iv": iv, "iv_52w_high": max(ivs),
                "iv_52w_low": min(ivs), "provider": "finnhub",
            })
        return pd.DataFrame(rows)

    def test_ingests_today_when_no_record_exists(self, svc):
        """When today has no stored record, provider is called and data is upserted."""
        today = date.today()
        raw_df = pd.DataFrame([{
            "recorded_at": today, "symbol": "SPY",
            "current_iv": 0.25, "provider": "finnhub",
        }])
        history_df = self._make_history_df([0.20, 0.22, 0.18, 0.24], today)

        mock_provider = MagicMock()
        mock_provider.fetch_iv_rank.return_value = raw_df
        svc.store.query_iv_rank_history.side_effect = [
            pd.DataFrame(),   # today check → empty
            history_df,       # prior history
            history_df,       # final return
        ]
        svc.router.iter_for_type.return_value = [mock_provider]

        result = svc.get_iv_rank("SPY", lookback_days=252)

        svc.store.upsert_iv_rank.assert_called_once()
        upserted = svc.store.upsert_iv_rank.call_args[0][0]
        assert upserted.iloc[0]["current_iv"] == pytest.approx(0.25)
        assert 0 <= upserted.iloc[0]["iv_rank"] <= 100
        assert 0 <= upserted.iloc[0]["iv_percentile"] <= 100

    def test_skips_ingest_when_today_already_stored(self, svc):
        """When today's row exists, provider is NOT called."""
        today = date.today()
        existing = pd.DataFrame([{
            "recorded_at": today, "symbol": "SPY",
            "iv_rank": 65.0, "iv_percentile": 70.0,
            "current_iv": 0.28, "iv_52w_high": 0.40,
            "iv_52w_low": 0.15, "provider": "finnhub",
        }])
        svc.store.query_iv_rank_history.side_effect = [existing, existing]

        svc.get_iv_rank("SPY")

        svc.router.iter_for_type.assert_not_called()
        svc.store.upsert_iv_rank.assert_not_called()

    def test_iv_rank_formula(self, svc):
        """iv_rank = (current - min) / (max - min) * 100."""
        today = date.today()
        # history: IV range 0.10 – 0.30, current = 0.20 → rank = 50
        raw_df = pd.DataFrame([{
            "recorded_at": today, "symbol": "SPY",
            "current_iv": 0.20, "provider": "finnhub",
        }])
        history = self._make_history_df([0.10, 0.15, 0.25, 0.30], today)

        mock_provider = MagicMock()
        mock_provider.fetch_iv_rank.return_value = raw_df
        svc.store.query_iv_rank_history.side_effect = [
            pd.DataFrame(), history, history
        ]
        svc.router.iter_for_type.return_value = [mock_provider]

        svc.get_iv_rank("SPY")

        upserted = svc.store.upsert_iv_rank.call_args[0][0]
        # all_ivs = [0.10, 0.15, 0.25, 0.30, 0.20] → min=0.10, max=0.30
        assert upserted.iloc[0]["iv_rank"] == pytest.approx(50.0, abs=0.1)

    def test_iv_percentile_formula(self, svc):
        """iv_percentile = % of history days where stored IV < current_iv."""
        today = date.today()
        # history: [0.10, 0.15, 0.20, 0.25], current = 0.22
        # days below current: 0.10, 0.15, 0.20 → 3/4 = 75%
        raw_df = pd.DataFrame([{
            "recorded_at": today, "symbol": "SPY",
            "current_iv": 0.22, "provider": "finnhub",
        }])
        history = self._make_history_df([0.10, 0.15, 0.20, 0.25], today)

        mock_provider = MagicMock()
        mock_provider.fetch_iv_rank.return_value = raw_df
        svc.store.query_iv_rank_history.side_effect = [
            pd.DataFrame(), history, history
        ]
        svc.router.iter_for_type.return_value = [mock_provider]

        svc.get_iv_rank("SPY")

        upserted = svc.store.upsert_iv_rank.call_args[0][0]
        assert upserted.iloc[0]["iv_percentile"] == pytest.approx(75.0, abs=0.1)

    def test_graceful_on_provider_failure(self, svc):
        """Provider exception must not raise — returns whatever history exists."""
        today = date.today()
        history = self._make_history_df([0.20, 0.22], today)

        svc.store.query_iv_rank_history.side_effect = [
            pd.DataFrame(),  # today check → empty
            history,         # final return
        ]
        svc.router.iter_for_type.return_value = []  # no providers available

        result = svc.get_iv_rank("SPY")

        svc.store.upsert_iv_rank.assert_not_called()
        assert isinstance(result, pd.DataFrame)
