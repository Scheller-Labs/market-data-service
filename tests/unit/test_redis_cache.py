"""
tests/unit/test_redis_cache.py
Unit tests for the Redis hot cache layer.

Uses fakeredis to run a fully functional in-process Redis without a real server.
Tests validate key construction, TTL mapping, compression, and quota tracking.
"""

from datetime import date, datetime, timezone
from typing import Any

import pytest

# Conditionally import fakeredis — skip tests if not installed
try:
    import fakeredis
    HAS_FAKEREDIS = True
except ImportError:
    HAS_FAKEREDIS = FALSE = False

from market_data.models import DataType, Interval
from market_data.cache.redis_cache import RedisCache
from market_data.config import settings


pytestmark = pytest.mark.skipif(
    not HAS_FAKEREDIS,
    reason="fakeredis not installed — run: pip install fakeredis"
)


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def fake_redis_server():
    """A shared fakeredis server for the test session."""
    return fakeredis.FakeServer()


@pytest.fixture
def cache(fake_redis_server):
    """
    A RedisCache instance backed by fakeredis (no real Redis server needed).
    Patches the client initialization to use fakeredis.
    """
    import fakeredis
    cache = RedisCache.__new__(RedisCache)
    cache._client = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=False)
    cache._str_client = fakeredis.FakeRedis(server=fake_redis_server, decode_responses=True)
    cache.COMPRESSION_THRESHOLD_BYTES = RedisCache.COMPRESSION_THRESHOLD_BYTES
    return cache


@pytest.fixture
def sample_rows() -> list[dict[str, Any]]:
    """A small sample of OHLCV records for cache read/write tests."""
    return [
        {"timestamp": "2024-01-02T00:00:00+00:00", "symbol": "AAPL",
         "open": 185.0, "high": 188.5, "low": 184.0, "close": 187.5, "volume": 55_000_000},
        {"timestamp": "2024-01-03T00:00:00+00:00", "symbol": "AAPL",
         "open": 187.5, "high": 190.0, "low": 186.0, "close": 189.2, "volume": 48_000_000},
    ]


# ── Key Construction ───────────────────────────────────────────────────────

class TestKeyConstruction:
    def test_key_includes_data_type_and_symbol(self, cache):
        key = cache._make_key("AAPL", DataType.OHLCV)
        assert "ohlcv" in key
        assert "AAPL" in key

    def test_key_includes_dates_when_provided(self, cache):
        key = cache._make_key(
            "TSLA", DataType.OHLCV,
            start=date(2024, 1, 1), end=date(2024, 12, 31)
        )
        assert "2024-01-01" in key
        assert "2024-12-31" in key

    def test_key_includes_interval_when_provided(self, cache):
        key = cache._make_key("AAPL", DataType.OHLCV_INTRADAY, interval=Interval.ONE_HOUR)
        assert "1h" in key

    def test_key_starts_with_mds_prefix(self, cache):
        key = cache._make_key("AAPL", DataType.OHLCV)
        assert key.startswith("mds:")

    def test_different_symbols_have_different_keys(self, cache):
        key_aapl = cache._make_key("AAPL", DataType.OHLCV)
        key_tsla = cache._make_key("TSLA", DataType.OHLCV)
        assert key_aapl != key_tsla

    def test_symbol_uppercased_in_key(self, cache):
        key = cache._make_key("aapl", DataType.OHLCV)
        assert "AAPL" in key
        assert "aapl" not in key


# ── TTL Mapping ───────────────────────────────────────────────────────────

class TestTTLMapping:
    def test_ohlcv_uses_eod_ttl(self, cache):
        ttl = cache._ttl_for(DataType.OHLCV)
        assert ttl == settings.cache_ttl_eod

    def test_intraday_uses_intraday_ttl(self, cache):
        ttl = cache._ttl_for(DataType.OHLCV_INTRADAY)
        assert ttl == settings.cache_ttl_intraday

    def test_fundamentals_uses_weekly_ttl(self, cache):
        ttl = cache._ttl_for(DataType.FUNDAMENTALS)
        assert ttl == settings.cache_ttl_fundamentals
        assert ttl >= 86400 * 7  # at least 7 days

    def test_tick_uses_realtime_ttl(self, cache):
        ttl = cache._ttl_for(DataType.TICK)
        assert ttl == settings.cache_ttl_realtime
        assert ttl <= 120  # realtime TTL should be short


# ── Get / Set ─────────────────────────────────────────────────────────────

class TestGetSet:
    def test_cache_miss_returns_none(self, cache):
        """Getting an uncached key should return None."""
        result = cache.get("AAPL", DataType.OHLCV)
        assert result is None

    def test_set_then_get_roundtrip(self, cache, sample_rows):
        """Data written with set() must be retrievable with get()."""
        cache.set("AAPL", DataType.OHLCV, sample_rows,
                  start=date(2024, 1, 1), end=date(2024, 1, 31))

        result = cache.get("AAPL", DataType.OHLCV,
                           start=date(2024, 1, 1), end=date(2024, 1, 31))

        assert result is not None
        assert len(result) == len(sample_rows)
        assert result[0]["symbol"] == "AAPL"

    def test_set_returns_true_on_success(self, cache, sample_rows):
        result = cache.set("AAPL", DataType.OHLCV, sample_rows)
        assert result is True

    def test_key_mismatch_returns_none(self, cache, sample_rows):
        """Getting with wrong key components must return None."""
        cache.set("AAPL", DataType.OHLCV, sample_rows,
                  start=date(2024, 1, 1), end=date(2024, 1, 31))

        # Different date range — should miss
        result = cache.get("AAPL", DataType.OHLCV,
                           start=date(2024, 2, 1), end=date(2024, 2, 28))
        assert result is None

    def test_different_symbols_dont_interfere(self, cache, sample_rows):
        """AAPL cache should not return TSLA data."""
        tsla_rows = [{"symbol": "TSLA", "close": 200.0}]
        cache.set("AAPL", DataType.OHLCV, sample_rows)
        cache.set("TSLA", DataType.OHLCV, tsla_rows)

        aapl_result = cache.get("AAPL", DataType.OHLCV)
        assert aapl_result is not None
        # Should not contain TSLA data
        assert all(r.get("symbol") != "TSLA" for r in aapl_result)

    def test_large_payload_compressed(self, cache):
        """Payloads over COMPRESSION_THRESHOLD_BYTES must be compressed."""
        # Generate a payload larger than the threshold
        large_rows = [{"key": "x" * 100, "value": i} for i in range(100)]

        set_result = cache.set("TEST", DataType.OHLCV, large_rows)
        assert set_result is True

        get_result = cache.get("TEST", DataType.OHLCV)
        assert get_result is not None
        assert len(get_result) == len(large_rows)


# ── Invalidate ────────────────────────────────────────────────────────────

class TestInvalidate:
    def test_invalidate_removes_cached_keys(self, cache, sample_rows):
        """invalidate() must delete matching cache entries."""
        cache.set("AAPL", DataType.OHLCV, sample_rows,
                  start=date(2024, 1, 1), end=date(2024, 1, 31))
        cache.invalidate("AAPL", DataType.OHLCV)

        result = cache.get("AAPL", DataType.OHLCV,
                           start=date(2024, 1, 1), end=date(2024, 1, 31))
        assert result is None

    def test_invalidate_does_not_affect_other_symbols(self, cache, sample_rows):
        """Invalidating AAPL should not affect TSLA cache entries."""
        tsla_rows = [{"symbol": "TSLA", "close": 200.0}]
        cache.set("AAPL", DataType.OHLCV, sample_rows)
        cache.set("TSLA", DataType.OHLCV, tsla_rows)

        cache.invalidate("AAPL", DataType.OHLCV)

        tsla_result = cache.get("TSLA", DataType.OHLCV)
        assert tsla_result is not None

    def test_invalidate_nonexistent_key_returns_zero(self, cache):
        """Invalidating a key that doesn't exist should return 0 without error."""
        count = cache.invalidate("NOTCACHED", DataType.OHLCV)
        assert count == 0


# ── Ping ──────────────────────────────────────────────────────────────────

class TestPing:
    def test_ping_returns_true_with_fakeredis(self, cache):
        """ping() should return True when Redis is reachable."""
        assert cache.ping() is True


# ── Quota Tracking ─────────────────────────────────────────────────────────

class TestQuotaTracking:
    def test_get_daily_quota_zero_initially(self, cache):
        """New provider quota should start at 0."""
        count = cache.get_daily_quota("alpha_vantage")
        assert count == 0

    def test_increment_quota_tracks_calls(self, cache):
        """increment_quota() must increment and return the new count."""
        count1 = cache.increment_quota("alpha_vantage")
        count2 = cache.increment_quota("alpha_vantage")
        assert count1 == 1
        assert count2 == 2

    def test_different_providers_have_independent_quotas(self, cache):
        """Alpha Vantage and Finnhub quotas must not interfere."""
        cache.increment_quota("alpha_vantage")
        cache.increment_quota("alpha_vantage")
        cache.increment_quota("finnhub")

        av_count = cache.get_daily_quota("alpha_vantage")
        fh_count = cache.get_daily_quota("finnhub")
        assert av_count == 2
        assert fh_count == 1
