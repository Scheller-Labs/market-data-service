"""
tests/unit/test_rate_limiter.py
Unit tests for the token bucket rate limiter.
"""

import time
import pytest

from market_data.providers.base import TokenBucketRateLimiter, RateLimitConfig, RateLimitExceeded


class TestTokenBucketRateLimiter:
    def test_first_call_always_allowed(self):
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(calls_per_minute=60, min_interval_seconds=1.0),
            provider_name="test"
        )
        limiter.check_and_record()  # should not raise

    def test_min_interval_enforced(self):
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(min_interval_seconds=5.0),
            provider_name="test"
        )
        limiter.check_and_record()  # first call ok
        with pytest.raises(RateLimitExceeded) as exc_info:
            limiter.check_and_record()  # too soon
        assert exc_info.value.reset_in_seconds > 0
        assert exc_info.value.provider == "test"

    def test_calls_per_minute_enforced(self):
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(calls_per_minute=3, min_interval_seconds=0.0),
            provider_name="test"
        )
        # First 3 calls should be fine
        limiter.check_and_record()
        limiter.check_and_record()
        limiter.check_and_record()
        # 4th should fail
        with pytest.raises(RateLimitExceeded):
            limiter.check_and_record()

    def test_rate_limit_exceeded_message(self):
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(min_interval_seconds=10.0),
            provider_name="alpha_vantage"
        )
        limiter.check_and_record()
        with pytest.raises(RateLimitExceeded) as exc_info:
            limiter.check_and_record()
        assert "alpha_vantage" in str(exc_info.value)
        assert "10" in str(exc_info.value)  # approximate wait time

    def test_no_limits_always_allows(self):
        limiter = TokenBucketRateLimiter(
            RateLimitConfig(),  # no limits set
            provider_name="test"
        )
        for _ in range(100):
            limiter.check_and_record()  # should never raise
