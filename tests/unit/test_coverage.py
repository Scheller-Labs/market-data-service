"""
tests/unit/test_coverage.py
Unit tests for coverage manifest and gap detection logic.
No database or network required — uses a temp SQLite file.
"""

import tempfile
from datetime import date

import pytest

from market_data.cache.coverage import CoverageManifest
from market_data.models import CoverageStatus, DataType, Interval


@pytest.fixture
def manifest(tmp_path):
    """Fresh CoverageManifest backed by a temp SQLite file."""
    db_path = str(tmp_path / "coverage.db")
    return CoverageManifest(db_path=db_path)


class TestGapDetection:
    def test_no_coverage_returns_full_gap(self, manifest):
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.MISSING
        assert len(report.gaps) == 1
        assert report.gaps[0].start == date(2024, 1, 1)
        assert report.gaps[0].end == date(2024, 12, 31)

    def test_complete_coverage_no_gaps(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31), "alpha_vantage")
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.COMPLETE
        assert len(report.gaps) == 0

    def test_gap_in_middle(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 3, 31), "alpha_vantage")
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 7, 1), date(2024, 12, 31), "alpha_vantage")
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.PARTIAL
        assert len(report.gaps) == 1
        assert report.gaps[0].start == date(2024, 4, 1)
        assert report.gaps[0].end == date(2024, 6, 30)

    def test_gap_at_start(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 6, 1), date(2024, 12, 31), "finnhub")
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.PARTIAL
        assert len(report.gaps) == 1
        assert report.gaps[0].start == date(2024, 1, 1)
        assert report.gaps[0].end == date(2024, 5, 31)

    def test_gap_at_end(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 9, 30), "alpha_vantage")
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.PARTIAL
        assert len(report.gaps) == 1
        assert report.gaps[0].start == date(2024, 10, 1)
        assert report.gaps[0].end == date(2024, 12, 31)

    def test_multiple_gaps(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 2, 1), date(2024, 3, 31), "alpha_vantage")
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 6, 1), date(2024, 7, 31), "alpha_vantage")
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.PARTIAL
        assert len(report.gaps) == 3
        # Gap 1: Jan 1 → Jan 31 (before first covered range)
        assert report.gaps[0].start == date(2024, 1, 1)
        assert report.gaps[0].end == date(2024, 1, 31)

    def test_overlapping_records_merge(self, manifest):
        """Two overlapping coverage records should behave as one continuous range."""
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 6, 30), "alpha_vantage")
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 5, 1), date(2024, 12, 31), "alpha_vantage")
        report = manifest.check(
            "AAPL", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.COMPLETE
        assert len(report.gaps) == 0

    def test_different_symbols_independent(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31), "alpha_vantage")
        report = manifest.check(
            "TSLA", DataType.OHLCV,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.MISSING

    def test_different_data_types_independent(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31), "alpha_vantage")
        report = manifest.check(
            "AAPL", DataType.FUNDAMENTALS,
            date(2024, 1, 1), date(2024, 12, 31)
        )
        assert report.status == CoverageStatus.MISSING


class TestCoverageRecord:
    def test_record_and_retrieve(self, manifest):
        manifest.record("SPY", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31),
                        "alpha_vantage", row_count=252)
        records = manifest.list_available(symbol="SPY")
        assert len(records) == 1
        assert records[0].symbol == "SPY"
        assert records[0].row_count == 252

    def test_upsert_updates_row_count(self, manifest):
        manifest.record("SPY", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31),
                        "alpha_vantage", row_count=100)
        manifest.record("SPY", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31),
                        "alpha_vantage", row_count=252)
        records = manifest.list_available(symbol="SPY")
        assert len(records) == 1
        assert records[0].row_count == 252

    def test_invalidate_removes_records(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31), "alpha_vantage")
        count = manifest.invalidate("AAPL", DataType.OHLCV)
        assert count == 1
        records = manifest.list_available(symbol="AAPL")
        assert len(records) == 0

    def test_list_filtered_by_type(self, manifest):
        manifest.record("AAPL", DataType.OHLCV,
                        date(2024, 1, 1), date(2024, 12, 31), "alpha_vantage")
        manifest.record("AAPL", DataType.FUNDAMENTALS,
                        date(2024, 1, 1), date(2024, 12, 31), "alpha_vantage")
        records = manifest.list_available(data_type=DataType.OHLCV)
        assert all(r.data_type == DataType.OHLCV for r in records)
