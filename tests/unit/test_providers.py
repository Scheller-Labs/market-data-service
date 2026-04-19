"""
tests/unit/test_providers.py
Unit tests for provider adapters — Alpha Vantage and Finnhub.

All HTTP calls are intercepted with httpx mock responses.
No real API calls are made; these tests validate the parsing/normalization logic.
"""

from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from market_data.models import DataType, Interval
from market_data.providers.alpha_vantage import AlphaVantageProvider
from market_data.providers.finnhub import FinnhubProvider
from market_data.providers.base import RateLimitConfig, TokenBucketRateLimiter


# ── Alpha Vantage Provider ─────────────────────────────────────────────────

class TestAlphaVantageProvider:
    """Tests for AlphaVantageProvider response parsing and schema normalization."""

    @pytest.fixture
    def provider(self):
        with patch("market_data.providers.alpha_vantage.settings") as mock_settings:
            mock_settings.alpha_vantage_api_key = "test_key"
            mock_settings.request_timeout_seconds = 30
            p = AlphaVantageProvider()
        return p

    @pytest.fixture
    def daily_response(self):
        """Realistic Alpha Vantage TIME_SERIES_DAILY JSON response (free tier)."""
        return {
            "Meta Data": {"2. Symbol": "AAPL"},
            "Time Series (Daily)": {
                "2024-01-05": {
                    "1. open": "185.00", "2. high": "188.50",
                    "3. low": "184.00", "4. close": "187.50",
                    "5. volume": "55000000",
                },
                "2024-01-04": {
                    "1. open": "182.00", "2. high": "186.00",
                    "3. low": "181.50", "4. close": "184.00",
                    "5. volume": "48000000",
                },
                "2023-12-29": {
                    # Out of requested range — should be filtered out
                    "1. open": "193.00", "2. high": "194.00",
                    "3. low": "191.00", "4. close": "192.50",
                    "5. volume": "30000000",
                },
            }
        }

    @pytest.fixture
    def fundamentals_response(self):
        return {
            "Symbol": "AAPL",
            "PERatio": "29.5",
            "EPS": "6.13",
            "RevenueTTM": "394330000000",
            "MarketCapitalization": "2950000000000",
            "DebtToEquityRatio": "1.73",
            "ReturnOnEquityTTM": "1.56",
            "Sector": "Technology",
            "Industry": "Consumer Electronics",
        }

    @pytest.fixture
    def earnings_response(self):
        return {
            "quarterlyEarnings": [
                {
                    "reportedDate": "2024-01-31",
                    "reportedEPS": "2.18",
                    "estimatedEPS": "2.10",
                    "surprise": "0.08",
                    "fiscalDateEnding": "2023-12-31",
                },
                {
                    "reportedDate": "2023-10-26",
                    "reportedEPS": "1.46",
                    "estimatedEPS": "1.39",
                    "surprise": "0.07",
                    "fiscalDateEnding": "2023-09-30",
                },
            ]
        }

    @pytest.fixture
    def dividends_response(self):
        return {
            "data": [
                {
                    "ex_dividend_date": "2024-02-09",
                    "amount": "0.24",
                    "payment_date": "2024-02-15",
                    "declaration_date": "2024-02-01",
                },
                {
                    "ex_dividend_date": "2023-11-10",
                    "amount": "0.24",
                    "payment_date": "2023-11-16",
                    "declaration_date": "2023-11-02",
                },
            ]
        }

    def test_fetch_daily_ohlcv_schema(self, provider, daily_response):
        """Daily OHLCV data must have canonical column names and correct types."""
        with patch.object(provider, "_get", return_value=daily_response):
            df = provider._fetch_daily("AAPL", date(2024, 1, 1), date(2024, 1, 31))

        assert not df.empty
        required = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "provider"}
        assert required.issubset(set(df.columns))
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["provider"].iloc[0] == "alpha_vantage"
        assert (df["close"] > 0).all()

    def test_fetch_daily_filters_date_range(self, provider, daily_response):
        """Rows outside [start, end] must be excluded from the result."""
        with patch.object(provider, "_get", return_value=daily_response):
            df = provider._fetch_daily("AAPL", date(2024, 1, 1), date(2024, 1, 31))

        # 2023-12-29 is before start — should not appear
        timestamps = pd.to_datetime(df["timestamp"]).dt.date
        assert all(d >= date(2024, 1, 1) for d in timestamps)

    def test_fetch_daily_sorted_ascending(self, provider, daily_response):
        """Returned DataFrame must be sorted by timestamp ascending."""
        with patch.object(provider, "_get", return_value=daily_response):
            df = provider._fetch_daily("AAPL", date(2024, 1, 1), date(2024, 1, 31))

        timestamps = pd.to_datetime(df["timestamp"])
        assert list(timestamps) == sorted(timestamps)

    def test_fetch_daily_empty_when_no_data_in_range(self, provider):
        """If no rows fall within the requested range, return empty DataFrame."""
        empty_response = {"Time Series (Daily)": {}}
        with patch.object(provider, "_get", return_value=empty_response):
            df = provider._fetch_daily("AAPL", date(2024, 1, 1), date(2024, 1, 31))

        assert df.empty

    def test_fetch_fundamentals_schema(self, provider, fundamentals_response):
        """Fundamentals response must parse all expected fields."""
        with patch.object(provider, "_get", return_value=fundamentals_response):
            df = provider._fetch_fundamentals("AAPL")

        assert not df.empty
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["pe_ratio"].iloc[0] == pytest.approx(29.5, rel=1e-3)
        assert df["eps"].iloc[0] == pytest.approx(6.13, rel=1e-3)
        assert df["sector"].iloc[0] == "Technology"
        assert df["provider"].iloc[0] == "alpha_vantage"

    def test_fetch_fundamentals_handles_missing_symbol(self, provider):
        """Empty/invalid response should return empty DataFrame, not raise."""
        with patch.object(provider, "_get", return_value={}):
            df = provider._fetch_fundamentals("UNKNOWN")
        assert df.empty

    def test_fetch_earnings_schema(self, provider, earnings_response):
        """Earnings response must include all required columns."""
        with patch.object(provider, "_get", return_value=earnings_response):
            df = provider._fetch_earnings("AAPL")

        assert not df.empty
        assert "report_date" in df.columns
        assert "eps_actual" in df.columns
        assert "eps_estimate" in df.columns
        assert "eps_surprise" in df.columns
        assert df["symbol"].iloc[0] == "AAPL"

    def test_fetch_earnings_drops_null_report_dates(self, provider):
        """Earnings rows with null report dates must be dropped."""
        response = {
            "quarterlyEarnings": [
                {"reportedDate": None, "reportedEPS": "2.18", "estimatedEPS": "2.10", "surprise": "0.08"},
                {"reportedDate": "2024-01-31", "reportedEPS": "2.18", "estimatedEPS": "2.10", "surprise": "0.08"},
            ]
        }
        with patch.object(provider, "_get", return_value=response):
            df = provider._fetch_earnings("AAPL")
        assert len(df) == 1
        assert df["report_date"].iloc[0] == date(2024, 1, 31)

    def test_fetch_dividends_schema(self, provider, dividends_response):
        """Dividends response must include ex_date, amount, and pay_date."""
        with patch.object(provider, "_get", return_value=dividends_response):
            df = provider._fetch_dividends("AAPL")

        assert not df.empty
        assert "ex_date" in df.columns
        assert "amount" in df.columns
        assert df["amount"].iloc[0] == pytest.approx(0.24, rel=1e-3)
        assert df["symbol"].iloc[0] == "AAPL"

    def test_rate_limit_note_raises_runtime_error(self, provider):
        """API rate limit 'Note' field in response should raise RuntimeError."""
        with patch.object(provider, "_get", side_effect=RuntimeError("Alpha Vantage rate limit note: Thank you...")):
            with pytest.raises(RuntimeError, match="rate limit"):
                provider._fetch_fundamentals("AAPL")

    def test_supported_data_types(self, provider):
        """Provider must support all declared data types."""
        supported = provider.supported_data_types()
        assert DataType.OHLCV in supported
        assert DataType.FUNDAMENTALS in supported
        assert DataType.EARNINGS in supported
        assert DataType.DIVIDENDS in supported

    def test_float_helper_handles_none_and_nan(self):
        """_float() must return None for non-numeric and NaN inputs."""
        assert AlphaVantageProvider._float(None) is None
        assert AlphaVantageProvider._float("None") is None
        assert AlphaVantageProvider._float("N/A") is None
        assert AlphaVantageProvider._float("3.14") == pytest.approx(3.14)

    def test_int_helper(self):
        """_int() must correctly parse integer strings and floats."""
        assert AlphaVantageProvider._int("1234567") == 1234567
        assert AlphaVantageProvider._int("1.5e9") == 1_500_000_000
        assert AlphaVantageProvider._int(None) is None


# ── Finnhub Provider ───────────────────────────────────────────────────────

class TestFinnhubProvider:
    """Tests for FinnhubProvider response parsing and schema normalization."""

    @pytest.fixture
    def provider(self):
        with patch("market_data.providers.finnhub.settings") as mock_settings:
            mock_settings.finnhub_api_key = "test_key"
            mock_settings.request_timeout_seconds = 30
            p = FinnhubProvider()
        return p

    @pytest.fixture
    def candle_response(self):
        """Realistic Finnhub /stock/candle response."""
        base_ts = int(datetime(2024, 1, 2, tzinfo=timezone.utc).timestamp())
        day_s = 86400
        return {
            "s": "ok",
            "t": [base_ts, base_ts + day_s, base_ts + 2 * day_s],
            "o": [185.0, 187.5, 189.2],
            "h": [188.5, 190.0, 191.0],
            "l": [184.0, 186.0, 187.8],
            "c": [187.5, 189.2, 188.0],
            "v": [55000000, 48000000, 51000000],
        }

    @pytest.fixture
    def options_response(self):
        """Minimal Finnhub /stock/option-chain response."""
        return {
            "data": [
                {
                    "expirationDate": "2024-02-16",
                    "options": {
                        "CALL": [
                            {
                                "strike": 190.0,
                                "bid": 2.50, "ask": 2.55, "lastPrice": 2.52,
                                "volume": 1000, "openInterest": 5000,
                                "impliedVolatility": 0.25,
                                "delta": 0.45, "gamma": 0.03,
                                "theta": -0.05, "vega": 0.15,
                            }
                        ],
                        "PUT": [
                            {
                                "strike": 190.0,
                                "bid": 1.80, "ask": 1.85, "lastPrice": 1.82,
                                "volume": 800, "openInterest": 4000,
                                "impliedVolatility": 0.27,
                                "delta": -0.55, "gamma": 0.03,
                                "theta": -0.05, "vega": 0.15,
                            }
                        ],
                    }
                }
            ]
        }

    @pytest.fixture
    def news_response(self):
        """Minimal Finnhub /company-news response."""
        return [
            {
                "datetime": int(datetime(2024, 1, 10, 9, 0, tzinfo=timezone.utc).timestamp()),
                "headline": "Apple Reports Record Q1 Earnings",
                "source": "Reuters",
                "url": "https://example.com/article1",
            },
            {
                "datetime": int(datetime(2024, 1, 11, 14, 0, tzinfo=timezone.utc).timestamp()),
                "headline": "Apple Vision Pro Sales Disappoint",
                "source": "Bloomberg",
                "url": "https://example.com/article2",
            },
        ]

    def test_fetch_ohlcv_schema(self, provider, candle_response):
        """OHLCV candle data must conform to the canonical MDS schema."""
        with patch.object(provider, "_get", return_value=candle_response):
            df = provider._fetch_ohlcv(
                "AAPL", date(2024, 1, 1), date(2024, 1, 31), Interval.ONE_DAY
            )

        assert not df.empty
        required = {"timestamp", "symbol", "open", "high", "low", "close", "volume", "provider"}
        assert required.issubset(set(df.columns))
        assert df["symbol"].iloc[0] == "AAPL"
        assert df["provider"].iloc[0] == "finnhub"
        assert len(df) == 3

    def test_fetch_ohlcv_no_data_returns_empty(self, provider):
        """Finnhub 'no_data' status must return empty DataFrame."""
        response = {"s": "no_data"}
        with patch.object(provider, "_get", return_value=response):
            df = provider._fetch_ohlcv(
                "AAPL", date(2024, 1, 1), date(2024, 1, 31), Interval.ONE_DAY
            )
        assert df.empty

    def test_fetch_ohlcv_sorted_ascending(self, provider, candle_response):
        """Candle data must be returned sorted by timestamp ascending."""
        # Reverse the order to test sort
        shuffled = candle_response.copy()
        shuffled["t"] = list(reversed(candle_response["t"]))
        shuffled["o"] = list(reversed(candle_response["o"]))
        shuffled["c"] = list(reversed(candle_response["c"]))
        shuffled["h"] = list(reversed(candle_response["h"]))
        shuffled["l"] = list(reversed(candle_response["l"]))
        shuffled["v"] = list(reversed(candle_response["v"]))

        with patch.object(provider, "_get", return_value=shuffled):
            df = provider._fetch_ohlcv(
                "AAPL", date(2024, 1, 1), date(2024, 1, 31), Interval.ONE_DAY
            )

        timestamps = pd.to_datetime(df["timestamp"])
        assert list(timestamps) == sorted(timestamps)

    def test_fetch_options_chain_schema(self, provider, options_response):
        """Options chain must include Greeks, strike, bid/ask, and option type."""
        with patch.object(provider, "_get", return_value=options_response):
            df = provider._fetch_options_chain("AAPL")

        assert not df.empty
        required = {"symbol", "expiration_date", "strike", "option_type", "bid", "ask"}
        assert required.issubset(set(df.columns))
        # Both CALL and PUT should be present
        option_types = set(df["option_type"].unique())
        assert "call" in option_types
        assert "put" in option_types

    def test_fetch_options_chain_invalid_expiry_skipped(self, provider):
        """Options with invalid expiration date format should be silently skipped."""
        bad_response = {
            "data": [{"expirationDate": "not-a-date", "options": {"CALL": [], "PUT": []}}]
        }
        with patch.object(provider, "_get", return_value=bad_response):
            df = provider._fetch_options_chain("AAPL")
        assert df.empty

    def test_fetch_news_sentiment_schema(self, provider, news_response):
        """News sentiment must include headline, source, and published_at."""
        with patch.object(provider, "_get", return_value=news_response):
            df = provider._fetch_news_sentiment("AAPL", date(2024, 1, 1), date(2024, 1, 31))

        assert not df.empty
        assert "headline" in df.columns
        assert "source" in df.columns
        assert "published_at" in df.columns
        assert df["symbol"].iloc[0] == "AAPL"

    def test_fetch_news_sentiment_empty_response(self, provider):
        """Empty news response should return empty DataFrame without error."""
        with patch.object(provider, "_get", return_value=[]):
            df = provider._fetch_news_sentiment("AAPL", date(2024, 1, 1), date(2024, 1, 31))
        assert df.empty

    def test_supported_data_types(self, provider):
        """Provider must correctly declare its capabilities."""
        supported = provider.supported_data_types()
        assert DataType.OHLCV in supported
        assert DataType.OPTIONS_CHAIN in supported
        assert DataType.NEWS_SENTIMENT in supported
        assert DataType.DIVIDENDS not in supported  # Finnhub doesn't support dividends

    def test_interval_mapping_daily(self, provider, candle_response):
        """ONE_DAY interval must map to Finnhub resolution 'D'."""
        with patch.object(provider, "_get", return_value=candle_response) as mock_get:
            provider._fetch_ohlcv("AAPL", date(2024, 1, 1), date(2024, 1, 31), Interval.ONE_DAY)
            call_args = mock_get.call_args
            assert call_args[0][1]["resolution"] == "D"

    def test_interval_mapping_intraday(self, provider, candle_response):
        """ONE_HOUR interval must map to Finnhub resolution '60'."""
        with patch.object(provider, "_get", return_value=candle_response) as mock_get:
            provider._fetch_ohlcv("AAPL", date(2024, 1, 1), date(2024, 1, 31), Interval.ONE_HOUR)
            call_args = mock_get.call_args
            assert call_args[0][1]["resolution"] == "60"


# ── BaseProvider Schema Enforcement ───────────────────────────────────────

class TestBaseProviderSchemaEnforcement:
    """Tests for the shared _enforce_ohlcv_schema() utility."""

    @pytest.fixture
    def provider(self):
        """Use AlphaVantage as a concrete implementation of BaseProvider."""
        with patch("market_data.providers.alpha_vantage.settings") as mock_settings:
            mock_settings.alpha_vantage_api_key = "test"
            mock_settings.request_timeout_seconds = 30
            return AlphaVantageProvider()

    def test_adds_symbol_column(self, provider):
        df = pd.DataFrame([{"timestamp": datetime.now(tz=timezone.utc),
                             "open": 100.0, "high": 105.0, "low": 99.0,
                             "close": 103.0, "volume": 1000}])
        result = provider._enforce_ohlcv_schema(df, "TSLA")
        assert result["symbol"].iloc[0] == "TSLA"

    def test_adds_provider_column(self, provider):
        df = pd.DataFrame([{"timestamp": datetime.now(tz=timezone.utc),
                             "open": 100.0, "high": 105.0, "low": 99.0,
                             "close": 103.0, "volume": 1000}])
        result = provider._enforce_ohlcv_schema(df, "TSLA")
        assert result["provider"].iloc[0] == "alpha_vantage"

    def test_timestamp_becomes_utc(self, provider):
        """Non-timezone-aware timestamps should be converted to UTC."""
        df = pd.DataFrame([{"timestamp": "2024-01-02",
                             "open": 100.0, "high": 105.0, "low": 99.0,
                             "close": 103.0, "volume": 1000}])
        result = provider._enforce_ohlcv_schema(df, "TSLA")
        assert result["timestamp"].dt.tz is not None

    def test_volume_coerced_to_int(self, provider):
        df = pd.DataFrame([{"timestamp": datetime.now(tz=timezone.utc),
                             "open": 100.0, "high": 105.0, "low": 99.0,
                             "close": 103.0, "volume": "55000000.0"}])
        result = provider._enforce_ohlcv_schema(df, "AAPL")
        assert result["volume"].dtype in [int, "int64"]

    def test_adj_close_added_when_missing(self, provider):
        df = pd.DataFrame([{"timestamp": datetime.now(tz=timezone.utc),
                             "open": 100.0, "high": 105.0, "low": 99.0,
                             "close": 103.0, "volume": 1000}])
        result = provider._enforce_ohlcv_schema(df, "AAPL")
        assert "adj_close" in result.columns


# ── FinnhubProvider IV Rank ────────────────────────────────────────────────

class TestFinnhubIVRank:
    """Tests for FinnhubProvider._fetch_iv_rank() and _compute_atm_iv()."""

    @pytest.fixture
    def provider(self):
        with patch("market_data.providers.finnhub.settings") as mock_settings:
            mock_settings.finnhub_api_key = "test_key"
            mock_settings.request_timeout_seconds = 30
            p = FinnhubProvider()
        return p

    def _make_chain(self, expiry_str: str, strike: float, call_iv: float, put_iv: float) -> dict:
        """Build a minimal /stock/option-chain response."""
        return {
            "data": [
                {
                    "expirationDate": expiry_str,
                    "options": {
                        "CALL": [{"strike": strike, "impliedVolatility": call_iv,
                                  "bid": 1.0, "ask": 1.1, "lastPrice": 1.05,
                                  "volume": 100, "openInterest": 500,
                                  "delta": 0.5, "gamma": 0.02, "theta": -0.03, "vega": 0.1}],
                        "PUT":  [{"strike": strike, "impliedVolatility": put_iv,
                                  "bid": 0.9, "ask": 1.0, "lastPrice": 0.95,
                                  "volume": 80, "openInterest": 400,
                                  "delta": -0.5, "gamma": 0.02, "theta": -0.03, "vega": 0.1}],
                    },
                }
            ]
        }

    def test_fetch_iv_rank_returns_expected_schema(self, provider):
        """_fetch_iv_rank must return a single-row DataFrame with required columns."""
        today = date.today()
        expiry = today + timedelta(days=14)
        chain = self._make_chain(expiry.isoformat(), 200.0, 0.30, 0.32)

        with patch.object(provider, "_get") as mock_get:
            mock_get.side_effect = lambda path, params: (
                {"c": 200.0} if "quote" in path else chain
            )
            df = provider._fetch_iv_rank("SPY")

        assert not df.empty
        assert list(df.columns) == ["recorded_at", "symbol", "current_iv", "provider"]
        assert df["symbol"].iloc[0] == "SPY"
        assert df["provider"].iloc[0] == "finnhub"
        assert df["recorded_at"].iloc[0] == today
        assert 0.28 < df["current_iv"].iloc[0] < 0.35  # average of 0.30 + 0.32

    def test_fetch_iv_rank_no_price_returns_empty(self, provider):
        """Missing current price must return empty DataFrame without raising."""
        with patch.object(provider, "_get", return_value={"c": None}):
            df = provider._fetch_iv_rank("SPY")
        assert df.empty

    def test_fetch_iv_rank_no_options_returns_empty(self, provider):
        """Empty options chain must return empty DataFrame without raising."""
        with patch.object(provider, "_get") as mock_get:
            mock_get.side_effect = lambda path, params: (
                {"c": 200.0} if "quote" in path else {"data": []}
            )
            df = provider._fetch_iv_rank("SPY")
        assert df.empty

    def test_compute_atm_iv_picks_nearest_expiry(self, provider):
        """_compute_atm_iv must prefer the nearest expiry in the 7-60 DTE window."""
        today = date.today()
        near_expiry = (today + timedelta(days=14)).isoformat()
        far_expiry  = (today + timedelta(days=45)).isoformat()
        expiry_data = [
            {"expirationDate": near_expiry, "options": {
                "CALL": [{"strike": 100.0, "impliedVolatility": 0.20}], "PUT": []}},
            {"expirationDate": far_expiry, "options": {
                "CALL": [{"strike": 100.0, "impliedVolatility": 0.40}], "PUT": []}},
        ]
        iv = provider._compute_atm_iv(expiry_data, 100.0, today)
        assert iv == pytest.approx(0.20, abs=1e-6)

    def test_compute_atm_iv_skips_outside_dte_window(self, provider):
        """Expirations with DTE < 7 or DTE > 60 must be ignored."""
        today = date.today()
        too_soon = (today + timedelta(days=3)).isoformat()
        too_far  = (today + timedelta(days=90)).isoformat()
        expiry_data = [
            {"expirationDate": too_soon, "options": {
                "CALL": [{"strike": 100.0, "impliedVolatility": 0.50}], "PUT": []}},
            {"expirationDate": too_far, "options": {
                "CALL": [{"strike": 100.0, "impliedVolatility": 0.60}], "PUT": []}},
        ]
        iv = provider._compute_atm_iv(expiry_data, 100.0, today)
        assert iv is None

    def test_compute_atm_iv_averages_call_and_put(self, provider):
        """ATM IV must average call and put IVs at the nearest strike."""
        today = date.today()
        expiry = (today + timedelta(days=14)).isoformat()
        expiry_data = [
            {"expirationDate": expiry, "options": {
                "CALL": [{"strike": 100.0, "impliedVolatility": 0.20}],
                "PUT":  [{"strike": 100.0, "impliedVolatility": 0.24}],
            }}
        ]
        iv = provider._compute_atm_iv(expiry_data, 100.0, today)
        assert iv == pytest.approx(0.22, abs=1e-6)

    def test_compute_atm_iv_uses_three_nearest_strikes(self, provider):
        """Up to 3 strikes closest to current price are included in the average."""
        today = date.today()
        expiry = (today + timedelta(days=14)).isoformat()
        # Current price 100 — strikes at 95, 100, 105 are the nearest 3
        expiry_data = [
            {"expirationDate": expiry, "options": {
                "CALL": [
                    {"strike": 80.0,  "impliedVolatility": 0.50},
                    {"strike": 95.0,  "impliedVolatility": 0.22},
                    {"strike": 100.0, "impliedVolatility": 0.20},
                    {"strike": 105.0, "impliedVolatility": 0.21},
                    {"strike": 120.0, "impliedVolatility": 0.55},
                ],
                "PUT": [],
            }}
        ]
        iv = provider._compute_atm_iv(expiry_data, 100.0, today)
        # Expected: avg of strikes 100, 95, 105 → (0.20 + 0.22 + 0.21) / 3
        assert iv == pytest.approx((0.20 + 0.22 + 0.21) / 3, abs=1e-6)
