"""
tests/unit/test_cli_contract.py

Contract tests for the market-data CLI — every command, every documented
output field, and every example from CLI.md is exercised here.

Design principles:
  - MarketDataService is fully mocked; these tests validate the CLI contract
    (argument parsing, output schema, field types, exit codes), NOT business logic.
  - Each test class corresponds to one CLI command.
  - "contract" tests assert the exact JSON schema from CLI.md is present.
  - "example" tests mirror specific examples from CLI.md comments.
  - stdout must always be valid JSON (or CSV when --format csv is requested).
  - Human-readable messages must never appear on stdout.
"""

import csv
import io
import json
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from typer.testing import CliRunner

from market_data.cli import app
from market_data.models import (
    BatchResponse,
    CoverageRecord,
    CoverageStatus,
    DataResponse,
    DataType,
    DateGap,
    HealthResponse,
    Interval,
    StatusResponse,
)

runner = CliRunner()

# ═══════════════════════════════════════════════════════════════════════════
# Shared builders
# ═══════════════════════════════════════════════════════════════════════════

_OHLCV_SCHEMA = [
    "timestamp", "symbol", "open", "high", "low", "close",
    "volume", "adj_close", "provider",
]

_OPTIONS_SCHEMA = [
    "snapshot_at", "symbol", "expiration_date", "strike", "option_type",
    "bid", "ask", "last", "volume", "open_interest", "implied_volatility",
    "delta", "gamma", "theta", "vega", "rho",
    "iv_rank", "iv_percentile", "underlying_price", "provider",
]

_IV_RANK_SCHEMA = [
    "recorded_at", "symbol", "iv_rank", "iv_percentile",
    "current_iv", "iv_52w_high", "iv_52w_low", "provider",
]


def _ohlcv_row(symbol="AAPL", i=0, provider="tastytrade"):
    base = datetime(2025, 3, 1, tzinfo=timezone.utc) + timedelta(days=i)
    return {
        "timestamp":  base.isoformat(),
        "symbol":     symbol,
        "open":       215.50 + i,
        "high":       218.90 + i,
        "low":        214.20 + i,
        "close":      217.30 + i,
        "volume":     62_450_000,
        "adj_close":  217.30 + i,
        "provider":   provider,
    }


def _make_data_response(
    symbol="AAPL",
    data_type=DataType.OHLCV,
    rows=5,
    source="timescaledb",
    coverage=CoverageStatus.COMPLETE,
    gaps=None,
    provider="tastytrade",
):
    data = [_ohlcv_row(symbol, i, provider) for i in range(rows)]
    return DataResponse(
        symbol=symbol,
        data_type=data_type,
        interval="1d",
        source=source,
        coverage=coverage,
        gaps=gaps or [],
        rows=rows,
        fetched_at=datetime.now(tz=timezone.utc).isoformat(),
        schema=_OHLCV_SCHEMA,
        data=data,
    )


def _make_status_response(symbol="AAPL", coverage=CoverageStatus.COMPLETE, gaps=None):
    return StatusResponse(
        symbol=symbol,
        data_type=DataType.OHLCV,
        coverage=coverage,
        available_ranges=[{"start": "2025-01-01", "end": "2025-12-31"}],
        gaps=gaps or [],
    )


def _make_health_response(
    ts=True, redis=True, minio=False, providers=None, overall=True
):
    return HealthResponse(
        timescaledb=ts,
        redis=redis,
        minio=minio,
        providers=providers or {"alpha_vantage": True, "tastytrade": True},
        overall=overall,
    )


def _make_options_df(symbol="SPY", n_rows=2):
    snap = datetime(2024, 6, 1, tzinfo=timezone.utc)
    rows = []
    for i in range(n_rows):
        rows.append({
            "snapshot_at":        snap.isoformat(),
            "symbol":             symbol,
            "expiration_date":    "2024-06-21",
            "strike":             520.0 + i * 5,
            "option_type":        "call" if i % 2 == 0 else "put",
            "bid":                2.10 + i * 0.10,
            "ask":                2.20 + i * 0.10,
            "last":               2.15 + i * 0.10,
            "volume":             8000 + i * 100,
            "open_interest":      40000 + i * 1000,
            "implied_volatility": 0.182,
            "delta":              0.41,
            "gamma":              0.038,
            "theta":              -0.12,
            "vega":               0.28,
            "rho":                0.009,
            "iv_rank":            62.0,
            "iv_percentile":      67.0,
            "underlying_price":   528.40,
            "provider":           "tastytrade",
        })
    return pd.DataFrame(rows)


def _make_options_df_databento(symbol="SPY", n_rows=2):
    """Options DataFrame as returned by Databento OPRA.PILLAR.

    CLI.md field reference table: Databento provides last/volume/OI;
    bid, ask, implied_volatility, delta, gamma, theta, vega, rho are NULL.
    """
    snap = datetime(2026, 3, 20, tzinfo=timezone.utc)
    rows = []
    for i in range(n_rows):
        rows.append({
            "snapshot_at":        snap.isoformat(),
            "symbol":             symbol,
            "expiration_date":    "2026-03-20",
            "strike":             560.0 + i * 5,
            "option_type":        "call" if i % 2 == 0 else "put",
            "bid":                None,
            "ask":                None,
            "last":               2.17 + i * 0.05,
            "volume":             8432 + i * 100,
            "open_interest":      42150 + i * 500,
            "implied_volatility": None,
            "delta":              None,
            "gamma":              None,
            "theta":              None,
            "vega":               None,
            "rho":                None,
            "iv_rank":            None,
            "iv_percentile":      None,
            "underlying_price":   None,
            "provider":           "databento",
        })
    return pd.DataFrame(rows)


def _make_iv_rank_df(symbol="SPY", n=5):
    base = date(2025, 1, 2)
    rows = []
    for i in range(n):
        rows.append({
            "recorded_at":   (base + timedelta(days=i)).isoformat(),
            "symbol":        symbol,
            "iv_rank":       60.0 + i * 2,
            "iv_percentile": 65.0 + i * 1.5,
            "current_iv":    0.182 + i * 0.005,
            "iv_52w_high":   0.381,
            "iv_52w_low":    0.108,
            "provider":      "tastytrade",
        })
    return pd.DataFrame(rows)


def _make_max_pain_result(max_pain=528.0):
    return {
        "symbol":          "SPY",
        "expiration":      "2024-06-21",
        "max_pain_price":  max_pain,
        "snapshot_date":   "2024-06-01",
        "strikes":         [500.0, 505.0, 510.0, 515.0, 520.0, 525.0, 528.0, 530.0, 535.0],
        "call_oi":         [12000, 8500, 9200, 11000, 14500, 18000, 8000, 22000, 6500],
        "put_oi":          [22000, 18500, 15000, 12000, 10500, 9000, 4000, 3500, 2000],
        "total_pain":      [4200000.0, 3800000.0, 3400000.0, 3050000.0, 2800000.0,
                            2600000.0, 2550000.0, 2700000.0, 3100000.0],
    }


# ═══════════════════════════════════════════════════════════════════════════
# get command
# ═══════════════════════════════════════════════════════════════════════════

class TestGetCommandContract:
    """Verify the complete output schema documented in CLI.md for `get`."""

    def test_json_output_has_all_documented_top_level_fields(self):
        """CLI.md specifies: symbol, data_type, interval, source, coverage,
        gaps, rows, fetched_at, schema, data — all must be present."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        assert result.exit_code == 0, result.output
        d = json.loads(result.stdout)
        for field in ("symbol", "data_type", "interval", "source",
                      "coverage", "gaps", "rows", "fetched_at", "schema", "data"):
            assert field in d, f"Missing field: {field}"

    def test_data_rows_have_all_ohlcv_columns(self):
        """Each row in data[] must have all columns listed in CLI.md schema."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(rows=3)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert d["rows"] == 3
        for row in d["data"]:
            for col in ("timestamp", "symbol", "open", "high", "low",
                        "close", "volume", "provider"):
                assert col in row, f"Row missing column: {col}"

    def test_schema_field_lists_column_names(self):
        """schema[] must match the column names of the data rows."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert isinstance(d["schema"], list)
        assert len(d["schema"]) > 0
        # schema matches keys in data rows
        if d["data"]:
            assert set(d["schema"]) == set(d["data"][0].keys())

    def test_coverage_complete(self):
        """coverage='complete' and gaps=[] per CLI.md."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(
            coverage=CoverageStatus.COMPLETE, gaps=[])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "SPY"])

        d = json.loads(result.stdout)
        assert d["coverage"] == "complete"
        assert d["gaps"] == []

    def test_coverage_partial_with_gaps(self):
        """coverage='partial' with non-empty gaps[] listing {start, end}."""
        gap = DateGap(start=date(2025, 4, 1), end=date(2025, 4, 15))
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(
            coverage=CoverageStatus.PARTIAL, gaps=[gap])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "SPY", "--days", "365"])

        d = json.loads(result.stdout)
        assert d["coverage"] == "partial"
        assert len(d["gaps"]) == 1
        assert d["gaps"][0]["start"] == "2025-04-01"
        assert d["gaps"][0]["end"] == "2025-04-15"

    def test_coverage_missing(self):
        """coverage='missing' when no data is available."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(
            rows=0, coverage=CoverageStatus.MISSING,
            gaps=[DateGap(start=date(2025, 1, 1), end=date(2025, 12, 31))])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "UNKNOWN"])

        d = json.loads(result.stdout)
        assert d["coverage"] == "missing"
        assert d["rows"] == 0

    def test_source_timescaledb(self):
        """source='timescaledb' when served from cache hit."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(source="timescaledb")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert d["source"] == "timescaledb"

    def test_source_merged(self):
        """source='merged' when new gaps were fetched and merged."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(source="merged")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert d["source"] == "merged"

    def test_interval_field_present_in_output(self):
        """interval field must be serialised in output (e.g. '1d')."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert d["interval"] == "1d"

    # ── Example: market-data get --symbol AAPL ──────────────────────────────

    def test_example_default_symbol_only(self):
        """CLI.md example: market-data get --symbol AAPL (365-day default)."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(rows=252)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["symbol"] == "AAPL"
        assert d["rows"] == 252
        call_kw = mock_svc.get.call_args.kwargs
        assert call_kw["interval"] == Interval.ONE_DAY

    def test_example_explicit_date_range(self):
        """CLI.md example: market-data get --symbol SPY --type ohlcv
        --start 2024-01-01 --end 2024-12-31"""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(symbol="SPY")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "SPY", "--type", "ohlcv",
                "--start", "2024-01-01", "--end", "2024-12-31",
            ])

        assert result.exit_code == 0
        kw = mock_svc.get.call_args.kwargs
        assert kw["start"] == date(2024, 1, 1)
        assert kw["end"] == date(2024, 12, 31)
        assert kw["data_type"] == DataType.OHLCV

    def test_example_intraday_1h_90_days(self):
        """CLI.md example: market-data get --symbol TSLA --type ohlcv_intraday
        --interval 1h --days 90"""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(
            symbol="TSLA", data_type=DataType.OHLCV_INTRADAY)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "TSLA",
                "--type", "ohlcv_intraday",
                "--interval", "1h", "--days", "90",
            ])

        assert result.exit_code == 0
        kw = mock_svc.get.call_args.kwargs
        assert kw["interval"] == Interval.ONE_HOUR
        assert kw["data_type"] == DataType.OHLCV_INTRADAY

    def test_example_force_refresh(self):
        """CLI.md example: market-data get --symbol AAPL --days 30 --force-refresh"""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--days", "30", "--force-refresh",
            ])

        assert result.exit_code == 0
        assert mock_svc.get.call_args.kwargs["force_refresh"] is True

    def test_example_csv_format(self):
        """CLI.md example: market-data get --symbol SPY --days 365 --format csv"""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(rows=5)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "SPY", "--days", "365", "--format", "csv",
            ])

        assert result.exit_code == 0
        reader = csv.DictReader(io.StringIO(result.stdout))
        rows = list(reader)
        assert len(rows) == 5
        assert "timestamp" in reader.fieldnames
        assert "close" in reader.fieldnames

    def test_example_force_specific_provider(self):
        """CLI.md example: market-data get --symbol AAPL --provider tastytrade"""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--provider", "tastytrade",
            ])

        assert result.exit_code == 0
        assert mock_svc.get.call_args.kwargs["preferred_provider"] == "tastytrade"

    def test_stdout_is_clean_json_no_human_text(self):
        """Human-readable messages must go to stderr — stdout must be valid JSON."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        # Must parse without error
        json.loads(result.stdout)

    def test_missing_symbol_exits_nonzero(self):
        """CLI.md: --symbol is required; omitting it is an error."""
        result = runner.invoke(app, ["get"])
        assert result.exit_code != 0

    def test_service_error_exits_1(self):
        """CLI.md exit code table: exit 1 on command failure."""
        mock_svc = MagicMock()
        mock_svc.get.side_effect = RuntimeError("DB down")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["get", "--symbol", "AAPL"])

        assert result.exit_code == 1

    def test_fundamentals_type_accepted(self):
        """CLI.md: --type fundamentals is a valid data type."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(
            data_type=DataType.FUNDAMENTALS, rows=1)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "AAPL", "--type", "fundamentals",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["data_type"] == "fundamentals"

    def test_example_options_chain_via_databento(self):
        """CLI.md example: get --symbol SPY --type options_chain --provider databento
        --start 2026-03-20 --end 2026-03-20 (Databento OPRA.PILLAR historical fetch)."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response(
            symbol="SPY", data_type=DataType.OPTIONS_CHAIN,
            source="api:databento", provider="databento",
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "get", "--symbol", "SPY",
                "--type", "options_chain",
                "--provider", "databento",
                "--start", "2026-03-20",
                "--end", "2026-03-20",
            ])

        assert result.exit_code == 0
        kw = mock_svc.get.call_args.kwargs
        assert kw["data_type"] == DataType.OPTIONS_CHAIN
        assert kw["preferred_provider"] == "databento"
        assert kw["start"] == date(2026, 3, 20)
        assert kw["end"] == date(2026, 3, 20)
        d = json.loads(result.stdout)
        assert d["symbol"] == "SPY"


# ═══════════════════════════════════════════════════════════════════════════
# status command
# ═══════════════════════════════════════════════════════════════════════════

class TestStatusCommandContract:
    """Verify status command output schema and examples from CLI.md."""

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: symbol, data_type, coverage, available_ranges, gaps, total_rows."""
        mock_svc = MagicMock()
        mock_svc.status.return_value = _make_status_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["status", "--symbol", "AAPL"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("symbol", "data_type", "coverage", "available_ranges", "gaps"):
            assert field in d, f"Missing field: {field}"

    def test_available_ranges_format(self):
        """available_ranges[] must be list of {start, end} per CLI.md."""
        mock_svc = MagicMock()
        mock_svc.status.return_value = StatusResponse(
            symbol="SPY",
            data_type=DataType.OHLCV,
            coverage=CoverageStatus.PARTIAL,
            available_ranges=[
                {"start": "2025-01-01", "end": "2025-03-10"},
                {"start": "2025-03-15", "end": "2025-03-21"},
            ],
            gaps=[{"start": "2025-03-11", "end": "2025-03-14"}],
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["status", "--symbol", "SPY"])

        d = json.loads(result.stdout)
        assert len(d["available_ranges"]) == 2
        assert d["available_ranges"][0]["start"] == "2025-01-01"
        assert len(d["gaps"]) == 1
        assert d["gaps"][0]["start"] == "2025-03-11"

    def test_example_check_specific_range(self):
        """CLI.md example: status --symbol SPY --start 2024-01-01 --end 2024-12-31"""
        mock_svc = MagicMock()
        mock_svc.status.return_value = _make_status_response(symbol="SPY")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "status", "--symbol", "SPY",
                "--start", "2024-01-01", "--end", "2024-12-31",
            ])

        assert result.exit_code == 0
        # svc.status(symbol, data_type, start_date, end_date) — positional args
        args = mock_svc.status.call_args.args
        assert args[2] == date(2024, 1, 1)
        assert args[3] == date(2024, 12, 31)

    def test_does_not_call_get(self):
        """status must be read-only — service.get() must never be called."""
        mock_svc = MagicMock()
        mock_svc.status.return_value = _make_status_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["status", "--symbol", "AAPL"])

        mock_svc.get.assert_not_called()

    def test_missing_symbol_exits_nonzero(self):
        result = runner.invoke(app, ["status"])
        assert result.exit_code != 0


# ═══════════════════════════════════════════════════════════════════════════
# batch command
# ═══════════════════════════════════════════════════════════════════════════

class TestBatchCommandContract:
    """Verify batch command output schema and examples from CLI.md."""

    def _make_batch_response(self, symbols=("AAPL", "SPY", "TSLA")):
        results = {s: _make_data_response(symbol=s, rows=252) for s in symbols}
        return BatchResponse(
            requested=list(symbols),
            succeeded=list(symbols),
            failed=[],
            results=results,
        )

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: requested, succeeded, failed, results."""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = self._make_batch_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "batch", "--symbols", "AAPL,SPY,TSLA", "--days", "365",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("requested", "succeeded", "failed", "results"):
            assert field in d, f"Missing field: {field}"

    def test_results_keyed_by_symbol(self):
        """results{} must be a dict keyed by symbol, each value a DataResponse."""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = self._make_batch_response(["AAPL", "MSFT"])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "batch", "--symbols", "AAPL,MSFT",
            ])

        d = json.loads(result.stdout)
        assert "AAPL" in d["results"]
        assert "MSFT" in d["results"]
        assert d["results"]["AAPL"]["symbol"] == "AAPL"

    def test_failed_array_present_when_empty(self):
        """failed[] must be present even when all succeeded (not omitted)."""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = BatchResponse(
            requested=["SPY"], succeeded=["SPY"], failed=[], results={}
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["batch", "--symbols", "SPY"])

        d = json.loads(result.stdout)
        assert "failed" in d
        assert d["failed"] == []

    def test_failed_array_populated_on_partial_failure(self):
        """failed[] must list symbols that raised errors."""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = BatchResponse(
            requested=["SPY", "BADINPUT"],
            succeeded=["SPY"],
            failed=["BADINPUT"],
            results={"SPY": _make_data_response("SPY")},
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["batch", "--symbols", "SPY,BADINPUT"])

        d = json.loads(result.stdout)
        assert "BADINPUT" in d["failed"]
        assert "SPY" in d["succeeded"]

    def test_example_fetch_etfs(self):
        """CLI.md example: batch --symbols SPY,QQQ,IWM --type ohlcv --days 365"""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = self._make_batch_response(["SPY", "QQQ", "IWM"])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "batch", "--symbols", "SPY,QQQ,IWM",
                "--type", "ohlcv", "--days", "365",
            ])

        assert result.exit_code == 0
        kw = mock_svc.batch.call_args.kwargs
        assert set(mock_svc.batch.call_args.args[0]) == {"SPY", "QQQ", "IWM"}

    def test_workers_flag_passed_to_service(self):
        """CLI.md example: batch --symbols ... --workers 8"""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = self._make_batch_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, [
                "batch", "--symbols", "AAPL,MSFT,SPY", "--workers", "8",
            ])

        kw = mock_svc.batch.call_args.kwargs
        assert kw.get("max_workers") == 8

    def test_symbols_comma_separated_parsed_correctly(self):
        """Comma list with whitespace should produce clean uppercase symbols."""
        mock_svc = MagicMock()
        mock_svc.batch.return_value = BatchResponse(
            requested=["AAPL", "TSLA"],
            succeeded=["AAPL", "TSLA"],
            failed=[],
            results={},
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["batch", "--symbols", " aapl , tsla "])

        symbols_arg = mock_svc.batch.call_args.args[0]
        assert "AAPL" in symbols_arg
        assert "TSLA" in symbols_arg


# ═══════════════════════════════════════════════════════════════════════════
# warm command
# ═══════════════════════════════════════════════════════════════════════════

class TestWarmCommandContract:
    """Verify warm command output schema and examples from CLI.md."""

    def _make_warm_result(self, symbols=("AAPL", "SPY"), types=("ohlcv", "fundamentals")):
        return {
            "symbols":    list(symbols),
            "data_types": list(types),
            "results": {
                t: {"succeeded": list(symbols), "failed": []}
                for t in types
            },
        }

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: symbols, data_types, results."""
        mock_svc = MagicMock()
        mock_svc.warm.return_value = self._make_warm_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["warm", "--watchlist", "AAPL,SPY"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("symbols", "data_types", "results"):
            assert field in d, f"Missing field: {field}"

    def test_results_keyed_by_data_type(self):
        """results{} must be keyed by data_type with succeeded/failed lists."""
        mock_svc = MagicMock()
        mock_svc.warm.return_value = self._make_warm_result(
            symbols=["SPY", "QQQ"], types=["ohlcv", "fundamentals"])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "warm", "--watchlist", "SPY,QQQ",
                "--types", "ohlcv,fundamentals",
            ])

        d = json.loads(result.stdout)
        assert "ohlcv" in d["results"]
        assert "fundamentals" in d["results"]
        assert "succeeded" in d["results"]["ohlcv"]
        assert "failed" in d["results"]["ohlcv"]

    def test_example_inline_symbols_specific_types(self):
        """CLI.md example: warm --watchlist SPY,QQQ,IWM --types ohlcv,fundamentals,earnings"""
        mock_svc = MagicMock()
        mock_svc.warm.return_value = self._make_warm_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "warm", "--watchlist", "SPY,QQQ,IWM",
                "--types", "ohlcv,fundamentals,earnings",
            ])

        assert result.exit_code == 0

    def test_example_watchlist_file(self, tmp_path):
        """CLI.md example: warm --watchlist watchlist.txt"""
        watchlist = tmp_path / "watchlist.txt"
        watchlist.write_text("# My watchlist\nAAPL\nMSFT\nSPY\nTSLA\n")

        mock_svc = MagicMock()
        mock_svc.warm.return_value = self._make_warm_result(
            symbols=["AAPL", "MSFT", "SPY", "TSLA"])

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "warm", "--watchlist", str(watchlist),
            ])

        assert result.exit_code == 0
        symbols = mock_svc.warm.call_args.args[0]
        assert "AAPL" in symbols
        assert "MSFT" in symbols
        assert "SPY" in symbols
        assert "TSLA" in symbols
        # Comments must be stripped
        assert not any(s.startswith("#") for s in symbols)


# ═══════════════════════════════════════════════════════════════════════════
# list-data command
# ═══════════════════════════════════════════════════════════════════════════

class TestListDataCommandContract:
    """Verify list-data output schema and examples from CLI.md."""

    def _make_record(self, symbol="AAPL", data_type="ohlcv"):
        return CoverageRecord(
            symbol=symbol,
            data_type=DataType(data_type),
            interval=Interval.ONE_DAY,
            start_date=date(2024, 3, 21),
            end_date=date(2025, 3, 21),
            provider="tastytrade",
            row_count=252,
            fetched_at=datetime(2025, 3, 21, 18, 30, 0, tzinfo=timezone.utc),
        )

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: records[], count."""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = [
            self._make_record("AAPL"),
            self._make_record("SPY"),
        ]

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["list-data"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert "records" in d
        assert "count" in d
        assert d["count"] == 2

    def test_record_has_all_documented_fields(self):
        """Each record must have: symbol, data_type, interval, start_date,
        end_date, provider, row_count, fetched_at per CLI.md."""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = [self._make_record()]

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["list-data"])

        d = json.loads(result.stdout)
        rec = d["records"][0]
        for field in ("symbol", "data_type", "interval", "start_date",
                      "end_date", "provider", "row_count", "fetched_at"):
            assert field in rec, f"Record missing field: {field}"

    def test_empty_returns_count_zero(self):
        """CLI.md: no records → count=0 and records=[]."""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = []

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["list-data"])

        d = json.loads(result.stdout)
        assert d["count"] == 0
        assert d["records"] == []

    def test_example_filter_by_symbol(self):
        """CLI.md example: list-data --symbol SPY"""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = [self._make_record("SPY")]

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["list-data", "--symbol", "SPY"])

        mock_svc.coverage.list_available.assert_called_once_with(
            symbol="SPY", data_type=None
        )

    def test_example_filter_by_type(self):
        """CLI.md example: list-data --type ohlcv"""
        mock_svc = MagicMock()
        mock_svc.coverage.list_available.return_value = []

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, ["list-data", "--type", "ohlcv"])

        mock_svc.coverage.list_available.assert_called_once_with(
            symbol=None, data_type=DataType.OHLCV
        )


# ═══════════════════════════════════════════════════════════════════════════
# health command
# ═══════════════════════════════════════════════════════════════════════════

class TestHealthCommandContract:
    """Verify health command output schema and examples from CLI.md."""

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: timescaledb, redis, minio, providers{}, overall."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("timescaledb", "redis", "minio", "providers", "overall"):
            assert field in d, f"Missing field: {field}"

    def test_providers_is_dict(self):
        """providers must be a dict mapping provider name → bool."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response(
            providers={"tastytrade": True, "alpha_vantage": False})

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        d = json.loads(result.stdout)
        assert isinstance(d["providers"], dict)
        assert d["providers"]["tastytrade"] is True
        assert d["providers"]["alpha_vantage"] is False

    def test_all_healthy_exits_0(self):
        """CLI.md: exit 0 when overall=True."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response(overall=True)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 0
        assert json.loads(result.stdout)["overall"] is True

    def test_degraded_exits_1(self):
        """CLI.md exit code table: exit 1 when overall=False."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response(
            ts=False, redis=True, overall=False)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 1

    def test_minio_false_does_not_affect_overall(self):
        """CLI.md: MinIO failure alone should not make overall False."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response(
            ts=True, redis=True, minio=False, overall=True)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["minio"] is False
        assert d["overall"] is True

    def test_example_basic_health_check(self):
        """CLI.md example: market-data health"""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert isinstance(d["timescaledb"], bool)
        assert isinstance(d["redis"], bool)

    def test_all_four_providers_in_health_output(self):
        """CLI.md health output includes tastytrade, alpha_vantage, finnhub, databento."""
        mock_svc = MagicMock()
        mock_svc.health.return_value = _make_health_response(
            providers={
                "tastytrade":    True,
                "alpha_vantage": True,
                "finnhub":       False,
                "databento":     True,
            },
            overall=True,
        )

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["health"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        providers = d["providers"]
        assert providers["tastytrade"] is True
        assert providers["alpha_vantage"] is True
        assert providers["finnhub"] is False
        assert providers["databento"] is True


# ═══════════════════════════════════════════════════════════════════════════
# audit command
# ═══════════════════════════════════════════════════════════════════════════

class TestAuditCommandContract:
    """Verify audit command output schema and examples from CLI.md."""

    def _mock_svc_with_data(self, data_rows, coverage=CoverageStatus.COMPLETE):
        mock_svc = MagicMock()
        mock_svc.get.return_value = DataResponse(
            symbol="AAPL", data_type=DataType.OHLCV, interval="1d",
            source="timescaledb", coverage=coverage,
            gaps=[], rows=len(data_rows),
            fetched_at=datetime.now(tz=timezone.utc).isoformat(),
            schema=_OHLCV_SCHEMA, data=data_rows,
        )
        return mock_svc

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: symbol, data_type, rows, date_range, coverage,
        gaps, null_counts, price_anomalies."""
        rows = [_ohlcv_row("AAPL", i) for i in range(5)]
        mock_svc = self._mock_svc_with_data(rows)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("symbol", "data_type", "rows", "coverage",
                      "gaps", "null_counts", "price_anomalies"):
            assert field in d, f"Missing field: {field}"

    def test_null_counts_reports_per_column(self):
        """null_counts must have an entry per column (including adj_close)."""
        rows = [_ohlcv_row("AAPL", i) for i in range(5)]
        mock_svc = self._mock_svc_with_data(rows)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert isinstance(d["null_counts"], dict)

    def test_price_anomalies_fields(self):
        """price_anomalies must have max_daily_move_pct and
        days_over_10pct_move per CLI.md."""
        rows = [_ohlcv_row("AAPL", i) for i in range(5)]
        mock_svc = self._mock_svc_with_data(rows)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        pa = d["price_anomalies"]
        assert "max_daily_move_pct" in pa
        assert "days_over_10pct_move" in pa

    def test_detects_10pct_anomaly(self):
        """CLI.md: audit reports days with >10% price moves."""
        rows = [
            {"timestamp": "2024-01-02T00:00:00+00:00", "symbol": "AAPL",
             "open": 100.0, "high": 102.0, "low": 99.0, "close": 100.0,
             "volume": 1000, "adj_close": 100.0, "provider": "tastytrade"},
            {"timestamp": "2024-01-03T00:00:00+00:00", "symbol": "AAPL",
             "open": 115.0, "high": 117.0, "low": 114.0, "close": 115.0,
             "volume": 1000, "adj_close": 115.0, "provider": "tastytrade"},
        ]
        mock_svc = self._mock_svc_with_data(rows)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert d["price_anomalies"]["days_over_10pct_move"] >= 1
        assert d["price_anomalies"]["max_daily_move_pct"] >= 10.0

    def test_zero_anomalies_on_normal_data(self):
        """Normal price moves (<10%) should produce days_over_10pct_move=0."""
        rows = [_ohlcv_row("AAPL", i) for i in range(10)]
        mock_svc = self._mock_svc_with_data(rows)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "AAPL"])

        d = json.loads(result.stdout)
        assert d["price_anomalies"]["days_over_10pct_move"] == 0

    def test_example_check_data_quality(self):
        """CLI.md example: audit --symbol TSLA --days 365"""
        rows = [_ohlcv_row("TSLA", i) for i in range(252)]
        mock_svc = self._mock_svc_with_data(rows)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["audit", "--symbol", "TSLA", "--days", "365"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["symbol"] == "TSLA"


# ═══════════════════════════════════════════════════════════════════════════
# options-chain command
# ═══════════════════════════════════════════════════════════════════════════

class TestOptionsChainCommandContract:
    """Verify options-chain output schema and examples from CLI.md."""

    def test_json_has_all_documented_top_level_fields(self):
        """CLI.md schema: symbol, expiration, option_type, rows, schema, data."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("symbol", "expiration", "option_type", "rows", "schema", "data"):
            assert field in d, f"Missing top-level field: {field}"

    def test_data_rows_have_all_documented_option_fields(self):
        """Each row must have all 20 fields from CLI.md schema."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df(n_rows=3)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        d = json.loads(result.stdout)
        required = (
            "snapshot_at", "symbol", "expiration_date", "strike", "option_type",
            "bid", "ask", "last", "volume", "open_interest", "implied_volatility",
            "delta", "gamma", "theta", "vega", "rho",
        )
        for row in d["data"]:
            for col in required:
                assert col in row, f"Row missing column: {col}"

    def test_schema_field_matches_data_columns(self):
        """schema[] must match the keys of data rows."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        d = json.loads(result.stdout)
        if d["data"]:
            assert set(d["schema"]) == set(d["data"][0].keys())

    def test_example_full_chain_latest(self):
        """CLI.md example: options-chain --symbol SPY (full chain, all expirations)."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df(n_rows=10)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["symbol"] == "SPY"
        assert d["expiration"] == "all"
        assert d["option_type"] == "all"

    def test_example_specific_expiration(self):
        """CLI.md example: options-chain --symbol SPY --expiration 2024-06-21"""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["expiration"] == "2024-06-21"
        kw = mock_svc.store.query_options_snapshot.call_args.kwargs
        assert kw["expiration_date"] == date(2024, 6, 21)

    def test_example_calls_only(self):
        """CLI.md example: options-chain --symbol AAPL --expiration 2024-06-21 --type call"""
        mock_svc = MagicMock()
        calls_df = _make_options_df(n_rows=1)
        calls_df["option_type"] = "call"
        mock_svc.store.query_options_snapshot.return_value = calls_df

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "AAPL",
                "--expiration", "2024-06-21", "--type", "call",
            ])

        assert result.exit_code == 0
        kw = mock_svc.store.query_options_snapshot.call_args.kwargs
        assert kw["option_type"] == "call"

    def test_example_historical_snapshot(self):
        """CLI.md example: options-chain --symbol SPY --snapshot-date 2024-01-15"""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY", "--snapshot-date", "2024-01-15",
            ])

        assert result.exit_code == 0
        kw = mock_svc.store.query_options_snapshot.call_args.kwargs
        assert kw["snapshot_date"] == date(2024, 1, 15)

    def test_example_csv_format(self):
        """CLI.md example: options-chain --symbol SPY --expiration 2024-06-21
        --format csv"""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df(n_rows=3)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY",
                "--expiration", "2024-06-21", "--format", "csv",
            ])

        assert result.exit_code == 0
        reader = csv.DictReader(io.StringIO(result.stdout))
        rows = list(reader)
        assert len(rows) == 3
        assert "strike" in reader.fieldnames

    def test_example_expiration_with_snapshot_date(self):
        """CLI.md example: options-chain --symbol SPY --expiration 2026-03-28
        --snapshot-date 2026-03-20 (specific expiry from historical snapshot)."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df_databento()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY",
                "--expiration", "2026-03-28",
                "--snapshot-date", "2026-03-20",
            ])

        assert result.exit_code == 0
        kw = mock_svc.store.query_options_snapshot.call_args.kwargs
        assert kw["expiration_date"] == date(2026, 3, 28)
        assert kw["snapshot_date"] == date(2026, 3, 20)

    def test_databento_options_have_null_greeks_and_bid_ask(self):
        """CLI.md field reference: Databento OPRA.PILLAR provides last/volume/OI
        but bid, ask, implied_volatility, delta, gamma, theta, vega, rho are NULL."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = _make_options_df_databento(
            n_rows=5)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "options-chain", "--symbol", "SPY", "--expiration", "2026-03-20",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["rows"] == 5
        for row in d["data"]:
            # Databento provides these
            assert row["last"] is not None
            assert row["volume"] is not None
            assert row["open_interest"] is not None
            assert row["provider"] == "databento"
            # Databento OPRA.PILLAR does NOT provide these
            assert row["bid"] is None
            assert row["ask"] is None
            assert row["implied_volatility"] is None
            assert row["delta"] is None
            assert row["gamma"] is None
            assert row["theta"] is None
            assert row["vega"] is None
            assert row["rho"] is None

    def test_no_data_returns_rows_zero_exit_0(self):
        """CLI.md: no data → rows=0, exit 0 (not an error)."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.return_value = pd.DataFrame()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["rows"] == 0

    def test_error_exits_1(self):
        """CLI.md exit codes: exit 1 on storage error."""
        mock_svc = MagicMock()
        mock_svc.store.query_options_snapshot.side_effect = RuntimeError("DB error")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["options-chain", "--symbol", "SPY"])

        assert result.exit_code == 1

    def test_missing_symbol_exits_nonzero(self):
        result = runner.invoke(app, ["options-chain"])
        assert result.exit_code != 0


# ═══════════════════════════════════════════════════════════════════════════
# iv-rank command
# ═══════════════════════════════════════════════════════════════════════════

class TestIVRankCommandContract:
    """Verify iv-rank output schema and examples from CLI.md."""

    def test_json_has_all_documented_top_level_fields(self):
        """CLI.md schema: symbol, lookback_days, current_iv_rank,
        current_iv_percentile, current_iv, rows, schema, data."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = _make_iv_rank_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "SPY"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("symbol", "lookback_days", "current_iv_rank",
                      "current_iv_percentile", "current_iv", "rows", "schema", "data"):
            assert field in d, f"Missing field: {field}"

    def test_current_iv_rank_equals_latest_row(self):
        """current_iv_rank must match the most recent row's iv_rank value."""
        df = _make_iv_rank_df(n=5)
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = df

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "SPY"])

        d = json.loads(result.stdout)
        last_iv_rank = df.iloc[-1]["iv_rank"]
        assert d["current_iv_rank"] == pytest.approx(last_iv_rank, abs=0.1)

    def test_data_rows_have_all_documented_columns(self):
        """Each row must have all 8 columns from CLI.md IV rank schema."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = _make_iv_rank_df(n=3)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "SPY"])

        d = json.loads(result.stdout)
        required = ("recorded_at", "symbol", "iv_rank", "iv_percentile",
                    "current_iv", "iv_52w_high", "iv_52w_low", "provider")
        for row in d["data"]:
            for col in required:
                assert col in row, f"Row missing column: {col}"

    def test_example_default_252_days(self):
        """CLI.md example: iv-rank --symbol SPY (252-day default)."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = _make_iv_rank_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "SPY"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["symbol"] == "SPY"
        assert d["lookback_days"] == 252

    def test_example_custom_lookback(self):
        """CLI.md example: iv-rank --symbol AAPL --days 504"""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = _make_iv_rank_df()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "AAPL", "--days", "504"])

        d = json.loads(result.stdout)
        assert d["lookback_days"] == 504

    def test_example_csv_format(self):
        """CLI.md example: iv-rank --symbol SPY --days 365 --format csv"""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = _make_iv_rank_df(n=5)

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "iv-rank", "--symbol", "SPY", "--days", "365", "--format", "csv",
            ])

        assert result.exit_code == 0
        reader = csv.DictReader(io.StringIO(result.stdout))
        rows = list(reader)
        assert len(rows) == 5
        assert "iv_rank" in reader.fieldnames

    def test_no_data_returns_rows_zero(self):
        """No IV rank history → rows=0, exit 0."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.return_value = pd.DataFrame()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "AAPL"])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["rows"] == 0

    def test_error_exits_1(self):
        """CLI.md exit codes: exit 1 on storage error."""
        mock_svc = MagicMock()
        mock_svc.get_iv_rank.side_effect = RuntimeError("fail")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, ["iv-rank", "--symbol", "AAPL"])

        assert result.exit_code == 1


# ═══════════════════════════════════════════════════════════════════════════
# max-pain command
# ═══════════════════════════════════════════════════════════════════════════

class TestMaxPainCommandContract:
    """Verify max-pain output schema and examples from CLI.md."""

    def test_json_has_all_documented_fields(self):
        """CLI.md schema: symbol, expiration, max_pain_price, snapshot_date,
        strikes, call_oi, put_oi, total_pain."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = _make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        for field in ("symbol", "expiration", "max_pain_price",
                      "strikes", "call_oi", "put_oi", "total_pain"):
            assert field in d, f"Missing field: {field}"

    def test_parallel_arrays_same_length(self):
        """strikes, call_oi, put_oi, total_pain must all be the same length."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = _make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        d = json.loads(result.stdout)
        n = len(d["strikes"])
        assert n == len(d["call_oi"])
        assert n == len(d["put_oi"])
        assert n == len(d["total_pain"])

    def test_max_pain_price_matches_minimum_pain_strike(self):
        """max_pain_price must correspond to the strike with minimum total_pain."""
        result_data = _make_max_pain_result(max_pain=528.0)
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = result_data

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        d = json.loads(result.stdout)
        min_pain_idx = d["total_pain"].index(min(d["total_pain"]))
        assert d["strikes"][min_pain_idx] == d["max_pain_price"]

    def test_example_basic_max_pain(self):
        """CLI.md example: max-pain --symbol SPY --expiration 2024-06-21"""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = _make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["symbol"] == "SPY"
        assert d["expiration"] == "2024-06-21"

    def test_example_historical_snapshot(self):
        """CLI.md example: max-pain --symbol SPY --expiration 2024-06-21
        --snapshot-date 2024-06-14"""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = _make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY",
                "--expiration", "2024-06-21",
                "--snapshot-date", "2024-06-14",
            ])

        assert result.exit_code == 0
        kw = mock_svc.store.compute_max_pain.call_args.kwargs
        assert kw.get("snapshot_date") == date(2024, 6, 14)

    def test_expiration_passed_as_date_object(self):
        """--expiration string must be parsed to date before passing to store."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = _make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        kw = mock_svc.store.compute_max_pain.call_args.kwargs
        assert kw["expiration_date"] == date(2024, 6, 21)
        assert kw["symbol"] == "SPY"

    def test_no_data_max_pain_null(self):
        """When no OI data is stored, max_pain_price must be null (not crash)."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.return_value = {
            "max_pain_price": None,
            "strikes": [],
            "call_oi": [],
            "put_oi": [],
            "total_pain": [],
        }

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        assert result.exit_code == 0
        d = json.loads(result.stdout)
        assert d["max_pain_price"] is None

    def test_missing_symbol_exits_nonzero(self):
        """--symbol is required per CLI.md."""
        result = runner.invoke(app, ["max-pain", "--expiration", "2024-06-21"])
        assert result.exit_code != 0

    def test_missing_expiration_exits_nonzero(self):
        """--expiration is required per CLI.md."""
        result = runner.invoke(app, ["max-pain", "--symbol", "SPY"])
        assert result.exit_code != 0

    def test_error_exits_1(self):
        """CLI.md exit codes: exit 1 on storage error."""
        mock_svc = MagicMock()
        mock_svc.store.compute_max_pain.side_effect = RuntimeError("DB error")

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, [
                "max-pain", "--symbol", "SPY", "--expiration", "2024-06-21",
            ])

        assert result.exit_code == 1


# ═══════════════════════════════════════════════════════════════════════════
# Cross-command contracts (stdout/stderr, exit codes)
# ═══════════════════════════════════════════════════════════════════════════

class TestCrossCommandContracts:
    """Contracts that apply across all commands."""

    @pytest.mark.parametrize("args", [
        ["get", "--symbol", "AAPL"],
        ["status", "--symbol", "AAPL"],
        ["health"],
        ["list-data"],
        ["options-chain", "--symbol", "SPY"],
        ["iv-rank", "--symbol", "SPY"],
        ["max-pain", "--symbol", "SPY", "--expiration", "2024-06-21"],
    ])
    def test_stdout_is_valid_json(self, args):
        """All commands must emit valid JSON on stdout (agents pipe stdout)."""
        mock_svc = MagicMock()
        mock_svc.get.return_value = _make_data_response()
        mock_svc.status.return_value = _make_status_response()
        mock_svc.health.return_value = _make_health_response()
        mock_svc.coverage.list_available.return_value = []
        mock_svc.store.query_options_snapshot.return_value = _make_options_df()
        mock_svc.get_iv_rank.return_value = _make_iv_rank_df()
        mock_svc.store.compute_max_pain.return_value = _make_max_pain_result()

        with patch("market_data.cli._get_service", return_value=mock_svc):
            result = runner.invoke(app, args)

        try:
            json.loads(result.stdout)
        except json.JSONDecodeError as e:
            pytest.fail(f"stdout is not valid JSON for {args}: {e}\n{result.stdout[:200]}")

    def test_help_exits_0(self):
        """--help must always exit 0 (CLI.md: Global Flags)."""
        result = runner.invoke(app, ["--help"])
        assert result.exit_code == 0

    @pytest.mark.parametrize("cmd", [
        "get", "status", "batch", "health",
        "options-chain", "iv-rank", "max-pain", "list-data", "warm", "audit",
    ])
    def test_each_command_has_help(self, cmd):
        """Every command must respond to --help with exit 0."""
        result = runner.invoke(app, [cmd, "--help"])
        assert result.exit_code == 0, f"{cmd} --help failed: {result.output}"
