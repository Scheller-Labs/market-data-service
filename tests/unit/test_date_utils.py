"""
tests/unit/test_date_utils.py
Unit tests for date utilities — market calendar, gap detection, trading day math.

All tests use a mocked exchange_calendars to avoid network dependency.
A fallback path (no exchange_calendars) is also exercised.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest

from market_data.utils.date_utils import (
    days_back,
    expand_to_market_gaps,
    is_market_day,
    market_days_between,
    next_market_day,
    trading_days_back,
)


# ── is_market_day() ───────────────────────────────────────────────────────

class TestIsMarketDay:
    def test_monday_is_market_day(self):
        """A typical Monday should be a market day (fallback: Mon-Fri)."""
        monday = date(2024, 1, 8)  # confirmed Monday
        assert monday.weekday() == 0  # sanity check
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            assert is_market_day(monday) is True

    def test_saturday_is_not_market_day(self):
        saturday = date(2024, 1, 6)
        assert saturday.weekday() == 5
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            assert is_market_day(saturday) is False

    def test_sunday_is_not_market_day(self):
        sunday = date(2024, 1, 7)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            assert is_market_day(sunday) is False

    def test_uses_exchange_calendars_when_available(self):
        """When exchange_calendars is available, it delegates to NYSE calendar."""
        mock_calendar = MagicMock()
        mock_calendar.is_session.return_value = True
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", True), \
             patch("market_data.utils.date_utils._NYSE", mock_calendar):
            result = is_market_day(date(2024, 1, 2))
        mock_calendar.is_session.assert_called_once_with("2024-01-02")
        assert result is True


# ── market_days_between() ─────────────────────────────────────────────────

class TestMarketDaysBetween:
    def test_week_contains_five_trading_days(self):
        """Mon-Fri range should produce 5 market days (using fallback logic)."""
        start = date(2024, 1, 8)  # Monday
        end = date(2024, 1, 12)   # Friday
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            days = market_days_between(start, end)
        assert len(days) == 5
        assert all(d.weekday() < 5 for d in days)

    def test_weekend_only_range_returns_empty(self):
        """Saturday–Sunday should return no market days."""
        saturday = date(2024, 1, 6)
        sunday = date(2024, 1, 7)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            days = market_days_between(saturday, sunday)
        assert days == []

    def test_single_market_day(self):
        """A single weekday should return a list with one date."""
        tuesday = date(2024, 1, 9)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            days = market_days_between(tuesday, tuesday)
        assert days == [tuesday]

    def test_result_is_sorted_ascending(self):
        """market_days_between must return dates in ascending order."""
        start = date(2024, 1, 8)
        end = date(2024, 1, 19)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            days = market_days_between(start, end)
        assert days == sorted(days)

    def test_delegates_to_exchange_calendars(self):
        """With exchange_calendars available, it uses NYSE sessions_in_range."""
        mock_calendar = MagicMock()
        import pandas as pd
        mock_calendar.sessions_in_range.return_value = pd.DatetimeIndex([
            "2024-01-02", "2024-01-03"
        ])
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", True), \
             patch("market_data.utils.date_utils._NYSE", mock_calendar):
            days = market_days_between(date(2024, 1, 2), date(2024, 1, 3))
        assert len(days) == 2


# ── next_market_day() ─────────────────────────────────────────────────────

class TestNextMarketDay:
    def test_next_day_after_monday_is_tuesday(self):
        monday = date(2024, 1, 8)
        expected_tuesday = date(2024, 1, 9)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            result = next_market_day(monday)
        assert result == expected_tuesday

    def test_next_day_after_friday_is_monday(self):
        """Friday's next market day must skip Saturday and Sunday."""
        friday = date(2024, 1, 12)
        expected_monday = date(2024, 1, 15)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            result = next_market_day(friday)
        assert result == expected_monday

    def test_next_day_after_saturday_is_monday(self):
        saturday = date(2024, 1, 6)
        expected_monday = date(2024, 1, 8)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            result = next_market_day(saturday)
        assert result == expected_monday


# ── days_back() ───────────────────────────────────────────────────────────

class TestDaysBack:
    def test_30_days_back_from_specific_date(self):
        base = date(2024, 3, 31)
        result = days_back(30, from_date=base)
        assert result == date(2024, 3, 1)

    def test_zero_days_back_is_same_date(self):
        base = date(2024, 6, 15)
        assert days_back(0, from_date=base) == base

    def test_uses_today_when_no_from_date(self):
        """days_back() with no from_date should use today."""
        from datetime import date as date_cls
        today = date_cls.today()
        result = days_back(7)
        assert result == today - timedelta(days=7)


# ── trading_days_back() ───────────────────────────────────────────────────

class TestTradingDaysBack:
    def test_roughly_252_trading_days_in_year(self):
        """trading_days_back(252) should produce a date approximately 1 year ago."""
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            base = date(2024, 12, 31)
            result = trading_days_back(252, from_date=base)
        # Should be approximately 1 year ago (within a few weeks)
        delta = (base - result).days
        assert 300 < delta < 420, f"Expected ~365 calendar days, got {delta}"

    def test_result_is_a_market_day(self):
        """trading_days_back() must snap the result to a market day."""
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            result = trading_days_back(5, from_date=date(2024, 1, 12))
        assert result.weekday() < 5  # must be Mon-Fri


# ── expand_to_market_gaps() ───────────────────────────────────────────────

class TestExpandToMarketGaps:
    def test_all_trading_days_covered_returns_empty(self):
        """No gaps when all trading days in range are covered."""
        start = date(2024, 1, 8)   # Monday
        end = date(2024, 1, 12)    # Friday
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            trading_days = market_days_between(start, end)
            result = expand_to_market_gaps(start, end, set(trading_days))
        assert result == []

    def test_no_coverage_returns_single_gap(self):
        """With no covered dates, the entire range should be one gap."""
        start = date(2024, 1, 8)
        end = date(2024, 1, 12)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            result = expand_to_market_gaps(start, end, set())
        assert len(result) == 1
        assert result[0][0] == start
        assert result[0][1] == end

    def test_middle_gap_detected(self):
        """Missing trading days in the middle should produce a gap."""
        start = date(2024, 1, 8)
        end = date(2024, 1, 12)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            all_days = market_days_between(start, end)
            # Cover Mon and Fri but not Tue-Thu
            covered = {all_days[0], all_days[-1]}
            result = expand_to_market_gaps(start, end, covered)
        assert len(result) >= 1

    def test_weekends_ignored_in_gap_calculation(self):
        """Weekend days should not create gaps in the result."""
        # Monday to Monday (skips a weekend)
        start = date(2024, 1, 8)
        end = date(2024, 1, 15)
        with patch("market_data.utils.date_utils._HAS_EXCHANGE_CALENDARS", False):
            all_trading = market_days_between(start, end)
            # Cover all trading days
            result = expand_to_market_gaps(start, end, set(all_trading))
        assert result == []  # No gaps when all trading days are covered
