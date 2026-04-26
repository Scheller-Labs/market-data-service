"""
market_data/cache/coverage.py
SQLite-backed coverage manifest.
Answers: "What data do we already have locally for symbol X, type Y, date range Z?"
"""

import sqlite3
import logging
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

from market_data.config import settings
from market_data.models import (
    CoverageRecord, CoverageReport, CoverageStatus,
    DataType, DateGap, Interval
)

logger = logging.getLogger(__name__)


CREATE_COVERAGE_TABLE = """
CREATE TABLE IF NOT EXISTS coverage_map (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    data_type       TEXT NOT NULL,
    interval        TEXT NOT NULL DEFAULT '',
    start_date      TEXT NOT NULL,
    end_date        TEXT NOT NULL,
    provider        TEXT NOT NULL,
    row_count       INTEGER,
    fetched_at      TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (symbol, data_type, interval, start_date, end_date, provider)
)
"""

CREATE_INDEX = """
CREATE INDEX IF NOT EXISTS idx_coverage_symbol_type
ON coverage_map (symbol, data_type, interval)
"""


class CoverageManifest:
    """
    Manages the local coverage manifest in SQLite.
    All date comparisons are conservative — if there's any doubt, we re-fetch.
    """

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or settings.coverage_db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self):
        with self._conn() as conn:
            conn.execute(CREATE_COVERAGE_TABLE)
            conn.execute(CREATE_INDEX)

    # ── Write ──────────────────────────────────────────────────────────────

    def record(
        self,
        symbol: str,
        data_type: DataType,
        start_date: date,
        end_date: date,
        provider: str,
        interval: Optional[Interval] = None,
        row_count: Optional[int] = None,
    ) -> None:
        """Record that we successfully fetched and stored this range."""
        with self._conn() as conn:
            conn.execute("""
                INSERT INTO coverage_map
                    (symbol, data_type, interval, start_date, end_date, provider, row_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (symbol, data_type, interval, start_date, end_date, provider)
                DO UPDATE SET
                    row_count = excluded.row_count,
                    fetched_at = datetime('now')
            """, (
                symbol.upper(),
                data_type.value,
                interval.value if interval else "",  # empty string avoids NULL UNIQUE issues
                start_date.isoformat(),
                end_date.isoformat(),
                provider,
                row_count,
            ))
        logger.debug(f"Recorded coverage: {symbol} {data_type} {start_date}→{end_date}")

    # ── Read ───────────────────────────────────────────────────────────────

    def get_covered_ranges(
        self,
        symbol: str,
        data_type: DataType,
        interval: Optional[Interval] = None,
    ) -> list[tuple[date, date]]:
        """Return all locally covered date ranges for this symbol/type.

        When interval is None, all stored intervals are included (wildcard).
        When interval is specified, only records for that exact interval are returned.
        """
        with self._conn() as conn:
            if interval is not None:
                rows = conn.execute("""
                    SELECT start_date, end_date FROM coverage_map
                    WHERE symbol = ? AND data_type = ? AND interval = ?
                    ORDER BY start_date
                """, (symbol.upper(), data_type.value, interval.value)).fetchall()
            else:
                rows = conn.execute("""
                    SELECT start_date, end_date FROM coverage_map
                    WHERE symbol = ? AND data_type = ?
                    ORDER BY start_date
                """, (symbol.upper(), data_type.value)).fetchall()

        ranges = [(date.fromisoformat(r["start_date"]), date.fromisoformat(r["end_date"])) for r in rows]
        return self._merge_ranges(ranges)

    def check(
        self,
        symbol: str,
        data_type: DataType,
        start_date: date,
        end_date: date,
        interval: Optional[Interval] = None,
    ) -> CoverageReport:
        """
        Check coverage for a requested date range.
        Returns a CoverageReport with status and any gaps.
        """
        covered = self.get_covered_ranges(symbol, data_type, interval)
        gaps = self._find_gaps(start_date, end_date, covered)

        if not gaps:
            status = CoverageStatus.COMPLETE
        elif len(gaps) == 1 and gaps[0].start == start_date and gaps[0].end == end_date:
            status = CoverageStatus.MISSING
        else:
            status = CoverageStatus.PARTIAL

        return CoverageReport(
            symbol=symbol,
            data_type=data_type,
            interval=interval,
            requested_start=start_date,
            requested_end=end_date,
            status=status,
            gaps=gaps,
            covered_ranges=covered,
        )

    # ── Gap Detection ──────────────────────────────────────────────────────

    def _find_gaps(
        self,
        requested_start: date,
        requested_end: date,
        covered_ranges: list[tuple[date, date]],
    ) -> list[DateGap]:
        """
        Given a requested range and covered ranges, return the gaps.
        Uses a sweep-line approach — O(n) after sorting.
        """
        if not covered_ranges:
            return [DateGap(start=requested_start, end=requested_end)]

        gaps = []
        cursor = requested_start

        for range_start, range_end in covered_ranges:
            if cursor > requested_end:
                break
            if range_start > cursor:
                # Gap between cursor and this range's start
                gap_end = min(range_start - timedelta(days=1), requested_end)
                gaps.append(DateGap(start=cursor, end=gap_end))
            # Advance cursor past this covered range
            if range_end >= cursor:
                cursor = range_end + timedelta(days=1)

        # Gap after all covered ranges
        if cursor <= requested_end:
            gaps.append(DateGap(start=cursor, end=requested_end))

        return gaps

    def _merge_ranges(
        self, ranges: list[tuple[date, date]]
    ) -> list[tuple[date, date]]:
        """Merge overlapping or adjacent date ranges."""
        if not ranges:
            return []

        sorted_ranges = sorted(ranges, key=lambda r: r[0])
        merged = [sorted_ranges[0]]

        for start, end in sorted_ranges[1:]:
            prev_start, prev_end = merged[-1]
            if start <= prev_end + timedelta(days=1):
                merged[-1] = (prev_start, max(prev_end, end))
            else:
                merged.append((start, end))

        return merged

    # ── Utilities ─────────────────────────────────────────────────────────

    def list_available(
        self,
        symbol: Optional[str] = None,
        data_type: Optional[DataType] = None,
    ) -> list[CoverageRecord]:
        """List all coverage records, optionally filtered."""
        query = "SELECT * FROM coverage_map WHERE 1=1"
        params: list = []
        if symbol:
            query += " AND symbol = ?"
            params.append(symbol.upper())
        if data_type:
            query += " AND data_type = ?"
            params.append(data_type.value)
        query += " ORDER BY symbol, data_type, start_date"

        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()

        return [
            CoverageRecord(
                symbol=r["symbol"],
                data_type=DataType(r["data_type"]),
                interval=Interval(r["interval"]) if r["interval"] else None,
                start_date=date.fromisoformat(r["start_date"]),
                end_date=date.fromisoformat(r["end_date"]),
                provider=r["provider"],
                row_count=r["row_count"],
            )
            for r in rows
        ]

    def invalidate(
        self,
        symbol: str,
        data_type: DataType,
        interval: Optional[Interval] = None,
    ) -> int:
        """Remove coverage records (used on force-refresh)."""
        with self._conn() as conn:
            cursor = conn.execute("""
                DELETE FROM coverage_map
                WHERE symbol = ? AND data_type = ? AND interval = ?
            """, (symbol.upper(), data_type.value, interval.value if interval else ""))
            return cursor.rowcount
