"""
market_data/cache/redis_cache.py
Redis hot cache — sub-millisecond reads for recently accessed data.
"""

import json
import logging
import zlib
from datetime import date
from typing import Optional, Any

import redis

from market_data.config import settings
from market_data.models import DataType, Interval

logger = logging.getLogger(__name__)


class RedisCache:
    """
    Hot cache for market data. Keys expire automatically per TTL config.
    Uses zlib compression on large payloads.

    Key schema: mds:{data_type}:{symbol}:{interval}:{start}:{end}
    """

    COMPRESSION_THRESHOLD_BYTES = 1024  # compress payloads > 1KB

    def __init__(self):
        self._client = redis.from_url(settings.redis_url, decode_responses=False)
        self._str_client = redis.from_url(settings.redis_url, decode_responses=True)

    def _make_key(
        self,
        symbol: str,
        data_type: DataType,
        start: Optional[date] = None,
        end: Optional[date] = None,
        interval: Optional[Interval] = None,
    ) -> str:
        parts = ["mds", data_type.value, symbol.upper()]
        if interval:
            parts.append(interval.value)
        if start:
            parts.append(start.isoformat())
        if end:
            parts.append(end.isoformat())
        return ":".join(parts)

    def _ttl_for(self, data_type: DataType) -> int:
        ttl_map = {
            DataType.OHLCV: settings.cache_ttl_eod,
            DataType.OHLCV_INTRADAY: settings.cache_ttl_intraday,
            DataType.OPTIONS_CHAIN: settings.cache_ttl_intraday,
            DataType.FUNDAMENTALS: settings.cache_ttl_fundamentals,
            DataType.NEWS_SENTIMENT: settings.cache_ttl_intraday,
            DataType.EARNINGS: settings.cache_ttl_eod,
            DataType.DIVIDENDS: settings.cache_ttl_eod,
            DataType.TICK: settings.cache_ttl_realtime,
            DataType.FUTURES_OHLCV: settings.cache_ttl_eod,
        }
        return ttl_map.get(data_type, settings.cache_ttl_eod)

    def get(
        self,
        symbol: str,
        data_type: DataType,
        start: Optional[date] = None,
        end: Optional[date] = None,
        interval: Optional[Interval] = None,
    ) -> Optional[list[dict[str, Any]]]:
        key = self._make_key(symbol, data_type, start, end, interval)
        try:
            raw = self._client.get(key)
            if raw is None:
                return None
            # Attempt decompression (compressed payload starts with zlib magic bytes)
            try:
                payload = zlib.decompress(raw)
            except zlib.error:
                payload = raw
            return json.loads(payload)
        except Exception as e:
            logger.warning(f"Redis get failed for {key}: {e}")
            return None

    def set(
        self,
        symbol: str,
        data_type: DataType,
        data: list[dict[str, Any]],
        start: Optional[date] = None,
        end: Optional[date] = None,
        interval: Optional[Interval] = None,
        ttl: Optional[int] = None,
    ) -> bool:
        key = self._make_key(symbol, data_type, start, end, interval)
        ttl = ttl or self._ttl_for(data_type)
        try:
            payload = json.dumps(data, default=str).encode()
            if len(payload) > self.COMPRESSION_THRESHOLD_BYTES:
                payload = zlib.compress(payload)
            self._client.setex(key, ttl, payload)
            logger.debug(f"Cached {symbol} {data_type} → {key} (TTL={ttl}s, {len(payload)} bytes)")
            return True
        except Exception as e:
            logger.warning(f"Redis set failed for {key}: {e}")
            return False

    def invalidate(
        self,
        symbol: str,
        data_type: DataType,
        interval: Optional[Interval] = None,
    ) -> int:
        """Delete all cache keys matching symbol + data_type."""
        pattern = f"mds:{data_type.value}:{symbol.upper()}*"
        try:
            keys = list(self._client.scan_iter(pattern))
            if keys:
                return self._client.delete(*keys)
            return 0
        except Exception as e:
            logger.warning(f"Redis invalidate failed for {pattern}: {e}")
            return 0

    def ping(self) -> bool:
        try:
            return self._client.ping()
        except Exception:
            return False

    # ── Rate Limit Quota Tracking ─────────────────────────────────────────

    def get_daily_quota(self, provider: str) -> int:
        """How many calls have been made to this provider today."""
        key = f"mds:quota:{provider}:{date.today().isoformat()}"
        try:
            val = self._str_client.get(key)
            return int(val) if val else 0
        except Exception:
            return 0

    def increment_quota(self, provider: str) -> int:
        """Increment daily call count for provider. Returns new count."""
        key = f"mds:quota:{provider}:{date.today().isoformat()}"
        try:
            pipe = self._str_client.pipeline()
            pipe.incr(key)
            pipe.expire(key, 86400)  # expire at end of day buffer
            results = pipe.execute()
            return results[0]
        except Exception as e:
            logger.warning(f"Quota increment failed for {provider}: {e}")
            return 0
