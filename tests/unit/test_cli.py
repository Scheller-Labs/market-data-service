"""
tests/unit/test_cli.py
Unit tests for the Typer CLI layer.

Uses typer.testing.CliRunner to invoke commands as subprocesses would.
The MarketDataService is fully mocked — these tests validate CLI behavior
(argument parsing, output format, exit codes) not business logic.
"""

import json
from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from market_data.cli import app
from market_data.models import (
    BatchResponse, CoverageStatus, DataResponse, DataType,
    DateGap, HealthResponse, Interval, StatusResponse,
)


runner = CliRunner()


# ── Fixtures ──────────────────────────────────────────────────────────────

def _make_data_response(
    symbol: str = "AAPL",
    rows: int = 5,
    source: str = "timescaledb",
    coverage: CoverageStatus = CoverageStatus.COMPLETE,
    gaps: list = None,
) -> DataResponse:
    """Build a minimal DataResponse suitable for CLI output tests."""
    data = [
        {
            "timestamp": f"2024-01-0{i+2}T00:00:00+00:00",
            "symbol": symbol, "open": 185.0, "high": 188.0,
            "low": 184.0, "close": 187.5, "volume": 50_000_000,
        }
        for i in range(rows)
    ]
    return DataResponse(
        symbol=symbol,
        data_type=DataType.OHLCV,
        interval="1d",
        source=source,
        coverage=coverage,
        gaps=gaps or [],
        rows=rows,
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        schema=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
        data=data,
    )


def _make_status_response(
    symbol: str = "AAPL",
    coverage: CoverageStatus = CoverageStatus.COMPLETE,
) -> StatusResponse:
    return StatusResponse(
        symbol=symbol,
        data_type=DataType.OHLCV,
        coverage=coverage,
        available_ranges=[{"start": "2024-01-01", "end": "2024-12-31"}],
        gaps=[],
    )


def _make_health_response(overall: bool = True) -> HealthResponse:
    return HealthResponse(
        timescaledb=True,
        redis=True,
        minio=True,
        providers={"alpha_vantage": True, "finnhub": True},
        overall=overall,
    )


# ── get command ───────────────────────────────────────────────────────────

class TestGetCommand:
    def test_get_basic_json_output(self):
        """get command with required args should output valid JSON."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--type", "ohlcv", "--days", "30"
            ])

        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["symbol"] == "AAPL"
        assert data["rows"] == 5
        assert "schema" in data
        assert "data" in data

    def test_get_with_explicit_date_range(self):
        """get --start and --end flags should be passed correctly to the service."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "TSLA",
                "--start", "2024-01-01", "--end", "2024-06-30"
            ])

        assert result.exit_code == 0
        # Verify service was called with correct dates
        call_kwargs = mock_svc.get.call_args
        assert call_kwargs.kwargs["start"] == date(2024, 1, 1)
        assert call_kwargs.kwargs["end"] == date(2024, 6, 30)

    def test_get_csv_format_output(self):
        """--format csv should produce CSV-formatted stdout, not JSON."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(rows=3)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--days", "30", "--format", "csv"
            ])

        assert result.exit_code == 0
        # CSV output starts with header row
        lines = result.stdout.strip().split("\n")
        assert "timestamp" in lines[0]  # header row
        assert len(lines) > 1           # at least one data row

    def test_get_force_refresh_flag(self):
        """--force-refresh should be passed through to the service call."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--days", "30", "--force-refresh"
            ])

        assert result.exit_code == 0
        call_kwargs = mock_svc.get.call_args
        assert call_kwargs.kwargs["force_refresh"] is True

    def test_get_service_error_exits_with_code_1(self):
        """Service exceptions should produce exit code 1."""
        mock_svc = MagicMock()
        mock_svc.get.side_effect = RuntimeError("Database unavailable")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--days", "30"
            ])

        assert result.exit_code == 1

    def test_get_missing_symbol_exits_nonzero(self):
        """Missing required --symbol should cause CLI error exit."""
        result = runner.invoke(app, ["get", "--days", "30"])
        assert result.exit_code != 0

    def test_get_default_interval_is_1d(self):
        """Default interval must be ONE_DAY when not specified."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["get", "--symbol", "AAPL", "--days", "30"])

        call_kwargs = mock_svc.get.call_args
        assert call_kwargs.kwargs["interval"] == Interval.ONE_DAY

    def test_get_with_gaps_shown_in_output(self):
        """Response with gaps should serialize them correctly in JSON output."""
        response = _make_data_response(
            coverage=CoverageStatus.PARTIAL,
            gaps=[DateGap(start=date(2024, 4, 1), end=date(2024, 4, 15))]
        )
        mock_svc = MagicMock()
        mock_svc.get.return_value = response

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL", "--days", "365"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["coverage"] == "partial"
        assert len(data["gaps"]) == 1
        assert data["gaps"][0]["start"] == "2024-04-01"


# ── status command ─────────────────────────────────────────────────────────

class TestStatusCommand:
    def test_status_json_output_structure(self):
        """status command should output valid JSON with coverage field."""
        mock_svc = MagicMock()
        mock_svc.status.return_value = _make_status_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "status", "--symbol", "AAPL", "--type", "ohlcv"
            ])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "coverage" in data
        assert "symbol" in data
        assert data["symbol"] == "AAPL"

    def test_status_does_not_call_get(self):
        """status command must not call service.get()."""
        mock_svc = MagicMock()
        mock_svc.status.return_value = _make_status_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["status", "--symbol", "AAPL"])

        mock_svc.get.assert_not_called()


# ── batch command ──────────────────────────────────────────────────────────

class TestBatchCommand:
    def test_batch_multiple_symbols(self):
        """batch command should call service.batch() with all symbols parsed."""
        mock_svc = MagicMock()
        resp_aapl = _make_data_response("AAPL")
        resp_tsla = _make_data_response("TSLA")
        mock_svc.batch.return_value = BatchResponse(
            requested=["AAPL", "TSLA"],
            succeeded=["AAPL", "TSLA"],
            failed=[],
            results={"AAPL": resp_aapl, "TSLA": resp_tsla},
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "batch", "--symbols", "AAPL,TSLA", "--days", "30"
            ])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert set(data["requested"]) == {"AAPL", "TSLA"}
        assert len(data["succeeded"]) == 2

    def test_batch_symbols_trimmed_and_uppercased(self):
        """Symbols with spaces or lowercase in --symbols should be normalized."""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = BatchResponse(
            requested=["SPY", "QQQ"], succeeded=["SPY", "QQQ"],
            failed=[], results={}
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["batch", "--symbols", " spy, qqq ", "--days", "30"])

        call_args = mock_svc.batch.call_args
        symbols_arg = call_args.args[0]
        assert "SPY" in symbols_arg
        assert "QQQ" in symbols_arg


# ── health command ─────────────────────────────────────────────────────────

class TestHealthCommand:
    def test_health_all_ok_exits_0(self):
        """health command should exit 0 when all components report healthy."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response(overall=True)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["overall"] is True
        assert data["timescaledb"] is True

    def test_health_degraded_exits_1(self):
        """health command should exit 1 when overall health is False."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = HealthResponse(
            timescaledb=False, redis=True, minio=False,
            providers={"alpha_vantage": True},
            overall=False,
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 1

    def test_health_providers_included_in_output(self):
        """health JSON should include per-provider status."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        data = json.loads(result.stdout)
        assert "providers" in data
        assert isinstance(data["providers"], dict)


# ── audit command ──────────────────────────────────────────────────────────

class TestAuditCommand:
    def test_audit_output_structure(self, sample_ohlcv_df):
        """audit should return null_counts, coverage, gaps, and price_anomalies."""
        mock_svc = MagicMock()
        rows = sample_ohlcv_df.to_dict("records")
        for row in rows:
            for k, v in row.items():
                if isinstance(v, datetime):
                    row[k] = v.isoformat()
        mock_svc.get.return_value = DataResponse(
            symbol="AAPL", data_type=DataType.OHLCV, interval="1d",
            source="timescaledb", coverage=CoverageStatus.COMPLETE,
            gaps=[], rows=len(rows),
            fetched_at=datetime.now(tz=timezone.utc).isoformat(),
            schema=list(sample_ohlcv_df.columns), data=rows,
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL", "--days", "30"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert "symbol" in data
        assert "rows" in data
        assert "coverage" in data
        assert "null_counts" in data
        assert "price_anomalies" in data

    def test_audit_detects_anomaly(self):
        """audit should report anomalies when price jumps > 10% in one day."""
        mock_svc = MagicMock()
        # Create data with a 15% daily jump
        data_rows = [
            {"timestamp": "2024-01-02T00:00:00+00:00", "symbol": "AAPL",
             "open": 100.0, "high": 102.0, "low": 99.0, "close": 100.0, "volume": 1000},
            {"timestamp": "2024-01-03T00:00:00+00:00", "symbol": "AAPL",
             "open": 115.0, "high": 117.0, "low": 114.0, "close": 115.0, "volume": 1000},
        ]
        mock_svc.get.return_value = DataResponse(
            symbol="AAPL", data_type=DataType.OHLCV, interval="1d",
            source="timescaledb", coverage=CoverageStatus.COMPLETE,
            gaps=[], rows=2, fetched_at=datetime.now(tz=timezone.utc).isoformat(),
            schema=["timestamp", "symbol", "open", "high", "low", "close", "volume"],
            data=data_rows,
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL", "--days", "30"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["price_anomalies"]["days_over_10pct_move"] >= 1


# ── warm command ──────────────────────────────────────────────────────────

class TestWarmCommand:
    def test_warm_from_comma_list(self, tmp_path):
        """warm --watchlist with inline symbols should work without a file."""
        mock_svc = MagicMock()
        mock_svc.warm.return_value = {
            "symbols": ["SPY", "QQQ"], "data_types": ["ohlcv"], "results": {}
        }

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "warm", "--watchlist", "SPY,QQQ", "--days", "90"
            ])

        assert result.exit_code == 0
        mock_svc.warm.assert_called_once()

    def test_warm_from_file(self, tmp_path):
        """warm --watchlist pointing to a file should read symbols line-by-line."""
        watchlist = tmp_path / "watchlist.txt"
        watchlist.write_text("AAPL\nMSFT\n# comment\nTSLA\n")

        mock_svc = MagicMock()
        mock_svc.warm.return_value = {"symbols": [], "data_types": [], "results": {}}

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "warm", "--watchlist", str(watchlist), "--days", "90"
            ])

        assert result.exit_code == 0
        call_args = mock_svc.warm.call_args
        symbols = call_args.args[0]
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert "TSLA" in symbols


# ── list-data command ─────────────────────────────────────────────────────

class TestListDataCommand:
    def test_list_empty_returns_count_zero(self):
        """list-data with no records should return count=0."""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = []

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["list-data"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["count"] == 0
        assert data["records"] == []

    def test_list_with_symbol_filter(self):
        """list-data --symbol should pass the filter to coverage.list_available()."""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = []

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["list-data", "--symbol", "AAPL"])

        mock_svc.coverage.list_available.assert_called_once_with(
            symbol="AAPL", data_type=None
        )


# ── options-chain command ─────────────────────────────────────────────────

class TestOptionsChainCommand:
    def _make_options_df(self):
        """Build a minimal options chain DataFrame for CLI output tests."""
        snap = datetime(2024, 1, 10, 16, 0, tzinfo=timezone.utc)
        return pd.DataFrame([
            {
                "snapshot_at": snap.isoformat(),
                "symbol": "SPY",
                "expiration_date": "2024-01-19",
                "strike": 470.0,
                "option_type": "call",
                "bid": 3.50, "ask": 3.60, "last": 3.55,
                "volume": 5000, "open_interest": 12000,
                "implied_volatility": 0.18,
                "delta": 0.42, "gamma": 0.05, "theta": -0.12,
                "vega": 0.15, "rho": 0.08,
                "iv_rank": 35.0, "iv_percentile": 40.0,
                "underlying_price": 468.5,
                "provider": "finnhub",
            },
            {
                "snapshot_at": snap.isoformat(),
                "symbol": "SPY",
                "expiration_date": "2024-01-19",
                "strike": 465.0,
                "option_type": "put",
                "bid": 2.80, "ask": 2.90, "last": 2.85,
                "volume": 4500, "open_interest": 10500,
                "implied_volatility": 0.19,
                "delta": -0.38, "gamma": 0.04, "theta": -0.11,
                "vega": 0.14, "rho": -0.06,
                "iv_rank": 35.0, "iv_percentile": 40.0,
                "underlying_price": 468.5,
                "provider": "finnhub",
            },
        ])

    def test_options_chain_json_output(self):
        """options-chain should output valid JSON with symbol, rows, and data fields."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = self._make_options_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["symbol"] == "SPY"
        assert data["rows"] == 2
        assert "data" in data
        assert "schema" in data

    def test_options_chain_passes_expiration_filter(self):
        """--expiration flag should be passed to query_options_snapshot."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = self._make_options_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, [
                "options-chain", "--symbol", "SPY",
                "--expiration", "2024-01-19"
            ])

        call_kwargs = mock_svc.store.query_options_snapshot.call_args
        assert call_kwargs.kwargs["expiration_date"] == date(2024, 1, 19)

    def test_options_chain_empty_data_returns_count_zero(self):
        """When no options data is stored, should return rows=0 without error."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = pd.DataFrame()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["rows"] == 0

    def test_options_chain_filters_by_type(self):
        """--type call should pass option_type='call' to query_options_snapshot."""
        mock_svc = MagicMock()
        calls_df = self._make_options_df()[self._make_options_df()["option_type"] == "call"]
        mock_svc.store.query_options_snapshot.return_value = calls_df

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY", "--type", "call"
            ])

        assert result.exit_code == 0
        call_kwargs = mock_svc.store.query_options_snapshot.call_args
        assert call_kwargs.kwargs["option_type"] == "call"

    def test_options_chain_error_exits_1(self):
        """Service errors should produce exit code 1."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.side_effect = RuntimeError("DB error")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        assert result.exit_code == 1


# ── iv-rank command ────────────────────────────────────────────────────────

class TestIVRankCommand:
    def _make_iv_rank_df(self):
        """Build a minimal IV rank history DataFrame for CLI tests."""
        rows = []
        base = date(2024, 1, 2)
        for i in range(5):
            rows.append({
                "recorded_at": (base + timedelta(days=i)).isoformat(),
                "symbol": "AAPL",
                "iv_rank": 35.0 + i * 2,
                "iv_percentile": 40.0 + i * 1.5,
                "current_iv": 0.22 + i * 0.01,
                "iv_52w_high": 0.55,
                "iv_52w_low": 0.15,
                "provider": "finnhub",
            })
        return pd.DataFrame(rows)

    def test_iv_rank_json_output(self):
        """iv-rank should output valid JSON with symbol, rows, and current IV rank."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = self._make_iv_rank_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "AAPL"])

        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["symbol"] == "AAPL"
        assert data["rows"] == 5
        assert "current_iv_rank" in data
        assert "data" in data

    def test_iv_rank_passes_date_range(self):
        """--days flag should control the lookback_days argument passed to get_iv_rank."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = self._make_iv_rank_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["iv-rank", "--symbol", "AAPL", "--days", "30"])

        mock_svc.get_iv_rank.assert_called_once_with(symbol="AAPL", lookback_days=30, force_refresh=False)

    def test_iv_rank_empty_data_returns_count_zero(self):
        """When no IV rank data is available, should return rows=0 without error."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = pd.DataFrame()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "AAPL"])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["rows"] == 0

    def test_iv_rank_error_exits_1(self):
        """Service errors should produce exit code 1."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.side_effect = RuntimeError("DB error")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "AAPL"])

        assert result.exit_code == 1


# ── max-pain command ────────────────────────────────────────────────────────

class TestMaxPainCommand:
    def _make_max_pain_result(self, max_pain: float = 470.0):
        """Build a realistic max pain result dict."""
        return {
            "max_pain_price": max_pain,
            "strikes": [460.0, 465.0, 470.0, 475.0, 480.0],
            "call_oi": [500, 800, 1200, 900, 400],
            "put_oi": [600, 700, 1100, 850, 350],
            "total_pain": [12000.0, 9500.0, 8200.0, 9800.0, 13500.0],
            "snapshot_date": "2024-01-10",
        }

    def test_max_pain_json_output(self):
        """max-pain should output valid JSON with max_pain_price and strikes."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = self._make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-01-19"
            ])

        assert result.exit_code == 0, result.output
        data = json.loads(result.stdout)
        assert data["symbol"] == "SPY"
        assert data["expiration"] == "2024-01-19"
        assert data["max_pain_price"] == 470.0
        assert "strikes" in data
        assert "total_pain" in data

    def test_max_pain_passes_expiration_to_store(self):
        """--expiration must be passed as a date object to compute_max_pain."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = self._make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-01-19"
            ])

        call_kwargs = mock_svc.store.compute_max_pain.call_args
        assert call_kwargs.kwargs["expiration_date"] == date(2024, 1, 19)
        assert call_kwargs.kwargs["symbol"] == "SPY"

    def test_max_pain_no_data_returns_none_price(self):
        """When no options data is stored, max_pain_price should be null."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = {
            "max_pain_price": None, "strikes": []
        }

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-01-19"
            ])

        assert result.exit_code == 0
        data = json.loads(result.stdout)
        assert data["max_pain_price"] is None

    def test_max_pain_missing_symbol_exits_nonzero(self):
        """Missing required --symbol should cause CLI error exit."""
        result = runner.invoke(app, ["max-pain", "--expiration", "2024-01-19"])
        assert result.exit_code != 0

    def test_max_pain_missing_expiration_exits_nonzero(self):
        """Missing required --expiration should cause CLI error exit."""
        result = runner.invoke(app, ["max-pain", "--symbol", "SPY"])
        assert result.exit_code != 0

    def test_max_pain_error_exits_1(self):
        """Service errors should produce exit code 1."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.side_effect = RuntimeError("DB error")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-01-19"
            ])

        assert result.exit_code == 1
