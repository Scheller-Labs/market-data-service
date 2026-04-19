"""
tests/unit/test_client.py
Unit tests for MarketDataClient — the agent-facing Python wrapper.

MarketDataService is fully mocked. No infrastructure (Redis, TimescaleDB,
MinIO, real providers) is required.
"""

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from market_data.client import MarketDataClient
from market_data.models import (
    BatchResponse,
    CoverageStatus,
    DataResponse,
    DataType,
    DateGap,
    HealthResponse,
    Interval,
    StatusResponse,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_data_response(rows: int = 5) -> DataResponse:
    """Build a minimal DataResponse with `rows` OHLCV rows."""
    schema = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    base = datetime(2024, 1, 2, tzinfo=timezone.utc)
    data = [
        {
            "timestamp": (base + timedelta(days=i)).isoformat(),
            "symbol":    "AAPL",
            "open":      180.0,
            "high":      182.0,
            "low":       179.0,
            "close":     181.0,
            "volume":    50_000_000,
        }
        for i in range(rows)
    ]
    return DataResponse(
        symbol="AAPL",
        data_type=DataType.OHLCV,
        interval="1d",
        source="timescaledb",
        coverage=CoverageStatus.COMPLETE,
        gaps=[],
        rows=rows,
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        schema=schema,
        data=data,
    )


def _make_empty_data_response() -> DataResponse:
    """Build a DataResponse with no rows."""
    schema = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
    return DataResponse(
        symbol="AAPL",
        data_type=DataType.OHLCV,
        interval="1d",
        source="timescaledb",
        coverage=CoverageStatus.MISSING,
        gaps=[],
        rows=0,
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        schema=schema,
        data=[],
    )


def _make_health_response(overall: bool = True) -> HealthResponse:
    return HealthResponse(
        timescaledb=overall,
        redis=overall,
        minio=overall,
        providers={"alpha_vantage": overall},
        overall=overall,
    )


def _make_status_response() -> StatusResponse:
    return StatusResponse(
        symbol="AAPL",
        data_type=DataType.OHLCV,
        coverage=CoverageStatus.COMPLETE,
        available_ranges=[{"start": "2024-01-01", "end": "2024-12-31"}],
        gaps=[],
        total_rows=252,
    )


# ── Fixture ───────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_svc():
    """A MagicMock replacing MarketDataService."""
    return MagicMock()


@pytest.fixture
def client(mock_svc):
    """
    A MarketDataClient whose internal _svc is replaced with mock_svc.

    We use __new__ to skip __init__ (which would instantiate MarketDataService),
    then inject the mock directly, mirroring the pattern in test_service.py.
    """
    c = MarketDataClient.__new__(MarketDataClient)
    c._svc = mock_svc
    return c


# ── get() ─────────────────────────────────────────────────────────────────────

class TestClientGet:
    def test_get_returns_dataframe(self, client, mock_svc):
        """get() must return a non-empty DataFrame when the service returns rows."""
        mock_svc.get.return_value = _make_data_response(rows=5)

        df = client.get("AAPL", "ohlcv", days=30)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5

    def test_get_empty_response_returns_empty_dataframe(self, client, mock_svc):
        """When the service returns data=[], get() must return an empty DataFrame
        with the correct columns."""
        mock_svc.get.return_value = _make_empty_data_response()

        df = client.get("AAPL", "ohlcv", days=30)

        assert isinstance(df, pd.DataFrame)
        assert df.empty
        # Columns must come from the response schema
        expected_cols = ["timestamp", "symbol", "open", "high", "low", "close", "volume"]
        assert list(df.columns) == expected_cols

    def test_get_uses_days_when_no_start_end(self, client, mock_svc):
        """
        When start/end are omitted, get() must compute start as today-days and
        end as today, then pass date objects to _svc.get.
        """
        mock_svc.get.return_value = _make_empty_data_response()
        today = date.today()
        days = 30

        client.get("AAPL", "ohlcv", days=days)

        mock_svc.get.assert_called_once()
        kwargs = mock_svc.get.call_args[1]
        assert kwargs["end"] == today
        # Allow 1-day tolerance for midnight boundary edge cases
        assert abs((kwargs["start"] - (today - timedelta(days=days))).days) <= 1

    def test_get_uses_explicit_start_end(self, client, mock_svc):
        """
        Explicit start/end strings must be converted to date objects and passed
        through to _svc.get.
        """
        mock_svc.get.return_value = _make_empty_data_response()

        client.get("AAPL", "ohlcv", start="2024-01-01", end="2024-12-31")

        mock_svc.get.assert_called_once()
        kwargs = mock_svc.get.call_args[1]
        assert kwargs["start"] == date(2024, 1, 1)
        assert kwargs["end"] == date(2024, 12, 31)

    def test_get_string_datatype_converted(self, client, mock_svc):
        """A string data_type must be converted to the DataType enum before calling _svc.get."""
        mock_svc.get.return_value = _make_empty_data_response()

        client.get("AAPL", "ohlcv", days=30)

        mock_svc.get.assert_called_once()
        kwargs = mock_svc.get.call_args[1]
        assert kwargs["data_type"] == DataType.OHLCV
        assert isinstance(kwargs["data_type"], DataType)

    def test_get_force_refresh_passed_through(self, client, mock_svc):
        """force_refresh=True must be forwarded to _svc.get."""
        mock_svc.get.return_value = _make_empty_data_response()

        client.get("AAPL", force_refresh=True)

        mock_svc.get.assert_called_once()
        kwargs = mock_svc.get.call_args[1]
        assert kwargs["force_refresh"] is True

    def test_get_timestamp_column_is_datetime(self, client, mock_svc):
        """The 'timestamp' column in the returned DataFrame must be datetime-typed."""
        mock_svc.get.return_value = _make_data_response(rows=3)

        df = client.get("AAPL", "ohlcv", days=30)

        assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])

    def test_get_preferred_provider_forwarded(self, client, mock_svc):
        """preferred_provider kwarg must reach _svc.get."""
        mock_svc.get.return_value = _make_empty_data_response()

        client.get("AAPL", preferred_provider="finnhub")

        kwargs = mock_svc.get.call_args[1]
        assert kwargs["preferred_provider"] == "finnhub"

    def test_get_interval_string_converted_to_enum(self, client, mock_svc):
        """A string interval must be converted to the Interval enum before _svc.get."""
        mock_svc.get.return_value = _make_empty_data_response()

        client.get("AAPL", interval="1d")

        kwargs = mock_svc.get.call_args[1]
        assert kwargs["interval"] == Interval.ONE_DAY
        assert isinstance(kwargs["interval"], Interval)


# ── status() ──────────────────────────────────────────────────────────────────

class TestClientStatus:
    def test_status_returns_dict(self, client, mock_svc):
        """status() must return a dict (the result of StatusResponse.model_dump())."""
        mock_svc.status.return_value = _make_status_response()

        result = client.status("AAPL", "ohlcv")

        assert isinstance(result, dict)
        assert result["symbol"] == "AAPL"
        assert result["coverage"] == CoverageStatus.COMPLETE

    def test_status_calls_service_with_datatype_enum(self, client, mock_svc):
        """status() must pass a DataType enum to _svc.status, not a raw string."""
        mock_svc.status.return_value = _make_status_response()

        client.status("AAPL", "ohlcv", days=90)

        mock_svc.status.assert_called_once()
        call_args = mock_svc.status.call_args[0]
        assert call_args[0] == "AAPL"
        assert call_args[1] == DataType.OHLCV

    def test_status_does_not_call_get(self, client, mock_svc):
        """status() must never trigger a data fetch (_svc.get must not be called)."""
        mock_svc.status.return_value = _make_status_response()

        client.status("AAPL")

        mock_svc.get.assert_not_called()


# ── batch() ───────────────────────────────────────────────────────────────────

class TestClientBatch:
    def test_batch_returns_dict_of_dataframes(self, client, mock_svc):
        """batch() must return a {symbol: DataFrame} mapping."""
        symbols = ["SPY", "QQQ"]
        batch_resp = BatchResponse(
            requested=symbols,
            succeeded=symbols,
            failed=[],
            results={sym: _make_data_response(rows=3) for sym in symbols},
        )
        mock_svc.batch.return_value = batch_resp

        result = client.batch(symbols, "ohlcv")

        assert isinstance(result, dict)
        assert set(result.keys()) == {"SPY", "QQQ"}
        for sym, df in result.items():
            assert isinstance(df, pd.DataFrame)
            assert len(df) == 3

    def test_batch_calls_service_batch(self, client, mock_svc):
        """batch() must delegate to _svc.batch once."""
        batch_resp = BatchResponse(
            requested=["SPY"],
            succeeded=["SPY"],
            failed=[],
            results={"SPY": _make_empty_data_response()},
        )
        mock_svc.batch.return_value = batch_resp

        client.batch(["SPY", "QQQ"], "ohlcv")

        mock_svc.batch.assert_called_once()

    def test_batch_empty_data_returns_empty_dataframes(self, client, mock_svc):
        """Symbols with no rows must map to empty DataFrames (not missing keys)."""
        symbols = ["AAPL"]
        batch_resp = BatchResponse(
            requested=symbols,
            succeeded=symbols,
            failed=[],
            results={"AAPL": _make_empty_data_response()},
        )
        mock_svc.batch.return_value = batch_resp

        result = client.batch(symbols, "ohlcv")

        assert "AAPL" in result
        assert result["AAPL"].empty

    def test_batch_datatype_string_converted(self, client, mock_svc):
        """A string data_type in batch() must be converted to a DataType enum."""
        batch_resp = BatchResponse(
            requested=["SPY"],
            succeeded=["SPY"],
            failed=[],
            results={"SPY": _make_empty_data_response()},
        )
        mock_svc.batch.return_value = batch_resp

        client.batch(["SPY"], "ohlcv")

        call_args = mock_svc.batch.call_args[0]
        # Second positional arg is data_type
        assert call_args[1] == DataType.OHLCV


# ── health() ──────────────────────────────────────────────────────────────────

class TestClientHealth:
    def test_health_returns_dict(self, client, mock_svc):
        """health() must return a dict (HealthResponse.model_dump())."""
        mock_svc.health.return_value = _make_health_response(overall=True)

        result = client.health()

        assert isinstance(result, dict)
        assert result["overall"] is True
        assert "timescaledb" in result
        assert "redis" in result
        assert "minio" in result

    def test_health_calls_service_health(self, client, mock_svc):
        """health() must delegate to _svc.health exactly once."""
        mock_svc.health.return_value = _make_health_response()

        client.health()

        mock_svc.health.assert_called_once()

    def test_health_reflects_partial_failure(self, client, mock_svc):
        """health() dict must faithfully reflect a False component status."""
        resp = HealthResponse(
            timescaledb=False,
            redis=True,
            minio=True,
            providers={"alpha_vantage": True},
            overall=False,
        )
        mock_svc.health.return_value = resp

        result = client.health()

        assert result["timescaledb"] is False
        assert result["overall"] is False


# ── _resolve_date (static helper) ─────────────────────────────────────────────

class TestResolveDate:
    def test_resolve_date_none_returns_none(self):
        assert MarketDataClient._resolve_date(None) is None

    def test_resolve_date_string_parsed(self):
        result = MarketDataClient._resolve_date("2024-06-15")
        assert result == date(2024, 6, 15)

    def test_resolve_date_date_object_passthrough(self):
        d = date(2024, 6, 15)
        assert MarketDataClient._resolve_date(d) is d
