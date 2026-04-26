"""
market_data/utils/date_utils.py
Date utilities — gap calculation, market day filtering.
"""

from datetime import date, timedelta
from typing import Optional

try:
    import exchange_calendars as xcals
    _NYSE = xcals.get_calendar("XNYS")
    _HAS_EXCHANGE_CALENDARS = True
except ImportError:
    _HAS_EXCHANGE_CALENDARS = False


def is_market_day(d: date) -> bool:
    """Return True if the date is a NYSE trading day."""
    if not _HAS_EXCHANGE_CALENDARS:
        return d.weekday() < 5  # fallback: Mon-Fri
    return _NYSE.is_session(d.isoformat())


def market_days_between(start: date, end: date) -> list[date]:
    """Return all NYSE trading days between start and end (inclusive)."""
    if not _HAS_EXCHANGE_CALENDARS:
        days = []
        cur = start
        while cur <= end:
            if cur.weekday() < 5:
                days.append(cur)
            cur += timedelta(days=1)
        return days

    sessions = _NYSE.sessions_in_range(start.isoformat(), end.isoformat())
    return [s.date() for s in sessions]


def expand_to_market_gaps(
    requested_start: date,
    requested_end: date,
    covered_dates: set[date],
) -> list[tuple[date, date]]:
    """
    Given a requested range and a set of covered trading days,
    return the missing sub-ranges as (start, end) tuples.
    Ignores non-trading days (weekends/holidays) in gap calculation.
    """
    all_trading_days = market_days_between(requested_start, requested_end)
    missing = sorted(d for d in all_trading_days if d not in covered_dates)

    if not missing:
        return []

    # Group consecutive missing days into ranges
    ranges = []
    range_start = missing[0]
    prev = missing[0]

    for d in missing[1:]:
        # Check if there's a trading day gap between prev and d
        next_trading = next_market_day(prev)
        if d > next_trading:
            ranges.append((range_start, prev))
            range_start = d
        prev = d

    ranges.append((range_start, prev))
    return ranges


def next_market_day(d: date) -> date:
    """Return the next NYSE trading day after d."""
    candidate = d + timedelta(days=1)
    while not is_market_day(candidate):
        candidate += timedelta(days=1)
        if (candidate - d).days > 10:
            break
    return candidate


def last_market_day(from_date: Optional[date] = None) -> date:
    """Return the most recent NYSE trading day on or before from_date (default: today)."""
    candidate = from_date or date.today()
    while not is_market_day(candidate):
        candidate -= timedelta(days=1)
    return candidate


def days_back(n: int, from_date: Optional[date] = None) -> date:
    """Return the date n calendar days before from_date (default: today)."""
    base = from_date or date.today()
    return base - timedelta(days=n)


def trading_days_back(n: int, from_date: Optional[date] = None) -> date:
    """Return the date approximately n trading days before from_date."""
    # Rough approximation: n trading days ≈ n * 1.4 calendar days
    # Then snap to the nearest trading day
    base = from_date or date.today()
    candidate = base - timedelta(days=int(n * 1.45))
    while not is_market_day(candidate):
        candidate -= timedelta(days=1)
    return candidate
