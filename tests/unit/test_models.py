"""
tests/unit/test_models.py
Unit tests for Pydantic models and schema validation.
"""

from datetime import date, datetime, timezone

import pandas as pd
import pytest

from market_data.models import (
    DataType, Interval, CoverageStatus, OptionType,
    OHLCVRow, DataResponse, CoverageReport, DateGap
)


class TestOHLCVRow:
    def test_valid_row(self):
        row = OHLCVRow(
            timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
            symbol="AAPL",
            open=185.0,
            high=188.0,
            low=184.5,
            close=187.5,
            volume=55_000_000,
        )
        assert row.symbol == "AAPL"
        assert row.close == 187.5
        assert row.adj_close is None

    def test_symbol_round_trips(self):
        row = OHLCVRow(
            timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
            symbol="aapl",  # lowercase input
            open=185.0, high=188.0, low=184.5, close=187.5, volume=1000
        )
        assert row.symbol == "aapl"  # model doesn't uppercase — that's the provider's job


class TestDataResponse:
    def test_empty_response(self):
        resp = DataResponse(
            symbol="AAPL",
            data_type=DataType.OHLCV,
            source="cache",
            coverage=CoverageStatus.MISSING,
            rows=0,
            fetched_at="2024-01-01T00:00:00Z",
            schema=[],
            data=[],
        )
        assert resp.rows == 0
        assert resp.gaps == []

    def test_json_serialization(self):
        resp = DataResponse(
            symbol="AAPL",
            data_type=DataType.OHLCV,
            source="timescaledb",
            coverage=CoverageStatus.COMPLETE,
            rows=252,
            fetched_at="2024-01-01T00:00:00Z",
            schema=["timestamp", "open", "high", "low", "close", "volume"],
            data=[{"timestamp": "2024-01-02T00:00:00+00:00", "open": 185.0,
                   "high": 188.0, "low": 184.5, "close": 187.5, "volume": 1000000}],
        )
        d = resp.model_dump()
        assert d["symbol"] == "AAPL"
        assert d["rows"] == 252


class TestCoverageReport:
    def test_gap_string_repr(self):
        gap = DateGap(start=date(2024, 4, 1), end=date(2024, 6, 30))
        assert "2024-04-01" in str(gap)
        assert "2024-06-30" in str(gap)

    def test_complete_no_gaps(self):
        report = CoverageReport(
            symbol="AAPL",
            data_type=DataType.OHLCV,
            requested_start=date(2024, 1, 1),
            requested_end=date(2024, 12, 31),
            status=CoverageStatus.COMPLETE,
        )
        assert report.gaps == []
        assert report.covered_ranges == []


class TestDataTypeEnum:
    def test_all_types_have_string_values(self):
        for dt in DataType:
            assert isinstance(dt.value, str)
            assert len(dt.value) > 0

    def test_from_string(self):
        assert DataType("ohlcv") == DataType.OHLCV
        assert DataType("options_chain") == DataType.OPTIONS_CHAIN

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            DataType("nonexistent_type")
