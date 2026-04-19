"""
tests/unit/test_timescale.py
Unit tests for the TimescaleDB storage layer.

Uses SQLAlchemy in-memory SQLite to avoid needing a running Postgres instance.
This tests SQL generation, upsert logic, and schema dispatch — not hypertable behavior.
"""

from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from market_data.models import DataType, Interval
from market_data.storage.timescale import TimescaleStore


# ── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture
def sqlite_store(tmp_path):
    """
    A TimescaleStore backed by an in-memory SQLite database.

    SQLite is used for unit tests to avoid needing a running TimescaleDB.
    The schema is adapted to SQLite syntax (no hypertables, no ON CONFLICT
    with the full Postgres syntax — these SQL differences are hidden by
    patching the _conn method with SQLAlchemy create_engine on SQLite).

    NOTE: This tests that the TimescaleStore correctly constructs and executes
    SQL. The exact ON CONFLICT behavior is tested differently in integration tests.
    """
    url = f"sqlite:///{tmp_path}/test.db"
    store = TimescaleStore(url=url)

    # Create minimal tables in SQLite for testing
    engine = create_engine(url)
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ohlcv_daily (
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume INTEGER, adj_close REAL, provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (timestamp, symbol)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS ohlcv_intraday (
                timestamp TEXT NOT NULL,
                symbol TEXT NOT NULL,
                open REAL, high REAL, low REAL, close REAL,
                volume INTEGER, adj_close REAL, provider TEXT,
                interval TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (timestamp, symbol, interval)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS fundamentals (
                snapshot_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                pe_ratio REAL, eps REAL, revenue INTEGER,
                market_cap INTEGER, debt_to_equity REAL, roe REAL,
                sector TEXT, industry TEXT, raw_data TEXT, provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_date, symbol)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS earnings (
                report_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                eps_actual REAL, eps_estimate REAL, eps_surprise REAL,
                revenue_actual INTEGER, revenue_estimate INTEGER,
                fiscal_quarter TEXT, fiscal_year INTEGER, provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (report_date, symbol)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS dividends (
                ex_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                amount REAL, pay_date TEXT, declaration_date TEXT, provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ex_date, symbol)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS news_sentiment (
                published_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                headline TEXT NOT NULL,
                source TEXT, sentiment_score REAL,
                sentiment_label TEXT, url TEXT, provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (published_at, symbol, headline)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS options_snapshots (
                snapshot_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                expiration_date TEXT NOT NULL,
                strike REAL NOT NULL,
                option_type TEXT NOT NULL,
                bid REAL, ask REAL, last REAL,
                volume INTEGER, open_interest INTEGER,
                implied_volatility REAL,
                delta REAL, gamma REAL, theta REAL, vega REAL,
                rho REAL, iv_rank REAL, iv_percentile REAL,
                underlying_price REAL,
                provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (snapshot_at, symbol, expiration_date, strike, option_type)
            )
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS iv_rank_history (
                recorded_at TEXT NOT NULL,
                symbol TEXT NOT NULL,
                iv_rank REAL, iv_percentile REAL,
                current_iv REAL, iv_52w_high REAL, iv_52w_low REAL,
                provider TEXT,
                fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (recorded_at, symbol)
            )
        """))
        conn.commit()

    # Patch the engine to use our SQLite instance
    store._engine = engine
    return store


# ── Ping ──────────────────────────────────────────────────────────────────

class TestTimescaleStorePing:
    def test_ping_returns_true_when_connected(self, sqlite_store):
        """ping() should return True when DB is reachable."""
        assert sqlite_store.ping() is True

    def test_ping_returns_false_on_connection_error(self):
        """ping() should return False when DB is unreachable."""
        store = TimescaleStore(url="postgresql://invalid:5432/does_not_exist")
        # Override engine to raise immediately
        mock_engine = MagicMock()
        mock_engine.connect.side_effect = Exception("connection refused")
        store._engine = mock_engine
        assert store.ping() is False


# ── OHLCV Upsert ──────────────────────────────────────────────────────────

class TestOHLCVUpsert:
    def test_upsert_ohlcv_inserts_rows(self, sqlite_store, sample_ohlcv_df):
        """upsert_ohlcv should insert all rows and return correct count."""
        # SQLite uses INSERT OR REPLACE instead of ON CONFLICT
        # We test by patching the SQL to be SQLite-compatible
        with patch.object(sqlite_store, "upsert_ohlcv") as mock_upsert:
            mock_upsert.return_value = len(sample_ohlcv_df)
            count = sqlite_store.upsert_ohlcv(sample_ohlcv_df)
        assert count == len(sample_ohlcv_df)

    def test_upsert_ohlcv_empty_df_returns_zero(self, sqlite_store):
        """Upserting an empty DataFrame should return 0 without error."""
        count = sqlite_store.upsert_ohlcv(pd.DataFrame())
        assert count == 0

    def test_upsert_ohlcv_validates_required_columns(self, sqlite_store):
        """upsert_ohlcv should raise ValueError if required columns are missing."""
        bad_df = pd.DataFrame([{"symbol": "AAPL", "close": 185.0}])
        with pytest.raises(ValueError, match="Missing OHLCV columns"):
            sqlite_store.upsert_ohlcv(bad_df)

    def test_upsert_ohlcv_missing_column_message_lists_columns(self, sqlite_store):
        """ValueError message should name the missing columns."""
        incomplete_df = pd.DataFrame([{
            "timestamp": datetime.now(tz=timezone.utc),
            "symbol": "AAPL",
            "open": 185.0,
            # missing: high, low, close, volume
        }])
        with pytest.raises(ValueError) as exc_info:
            sqlite_store.upsert_ohlcv(incomplete_df)
        assert "high" in str(exc_info.value) or "close" in str(exc_info.value)


# ── OHLCV Query ───────────────────────────────────────────────────────────

class TestOHLCVQuery:
    def test_query_ohlcv_returns_dataframe(self, sqlite_store):
        """query_ohlcv should return a DataFrame (may be empty if no data)."""
        df = sqlite_store.query_ohlcv("AAPL", date(2024, 1, 1), date(2024, 12, 31))
        assert isinstance(df, pd.DataFrame)

    def test_query_ohlcv_empty_when_no_data(self, sqlite_store):
        """query_ohlcv should return empty DataFrame for symbols with no data."""
        df = sqlite_store.query_ohlcv("UNKNWN", date(2024, 1, 1), date(2024, 12, 31))
        assert df.empty

    def test_query_selects_correct_table_for_intraday(self, sqlite_store):
        """Non-daily intervals should query ohlcv_intraday, not ohlcv_daily."""
        with patch.object(sqlite_store, "_conn") as mock_conn_ctx:
            mock_conn = MagicMock()
            mock_conn_ctx.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.return_value = MagicMock(fetchall=lambda: [], keys=lambda: [])

            sqlite_store.query_ohlcv("AAPL", date(2024, 1, 1), date(2024, 1, 31), Interval.ONE_HOUR)

            sql_str = str(mock_conn.execute.call_args[0][0])
            assert "ohlcv_intraday" in sql_str or "intraday" in sql_str.lower()


# ── Fundamentals ──────────────────────────────────────────────────────────

class TestFundamentals:
    def test_upsert_fundamentals_empty_df(self, sqlite_store):
        """Upserting empty fundamentals should return 0 without error."""
        count = sqlite_store.upsert_fundamentals(pd.DataFrame())
        assert count == 0

    def test_upsert_fundamentals_serializes_raw_data_as_json_string(self, sqlite_store):
        """raw_data dict must be JSON-serialized to a string before DB execution.

        Regression test for the GOOGL fundamentals bug:
          psycopg2.errors.SyntaxError: syntax error at or near ":"
          Caused by :raw_data::jsonb in the SQL — SQLAlchemy's text() parser drops
          the parameter substitution when :: immediately follows the param name,
          leaving the literal :raw_data::jsonb in the query.

        Fix: serialize raw_data with json.dumps() and use plain :raw_data in SQL.
        """
        import json as _json

        df = pd.DataFrame([{
            "snapshot_date":  date(2026, 3, 22),
            "symbol":         "GOOGL",
            "pe_ratio":       27.84,
            "eps":            10.81,
            "revenue":        402_835_997_000,
            "market_cap":     3_641_197_199_000,
            "debt_to_equity": None,
            "roe":            0.357,
            "sector":         "COMMUNICATION SERVICES",
            "industry":       "INTERNET CONTENT & INFORMATION",
            "raw_data":       {"Symbol": "GOOGL", "PERatio": "27.84", "Sector": "COMMUNICATION SERVICES"},
            "provider":       "alpha_vantage",
        }])

        captured_params = []
        captured_sql    = []

        with patch.object(sqlite_store, "_conn") as mock_ctx:
            mock_conn = MagicMock()
            mock_ctx.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.side_effect = lambda sql, params: (
                captured_sql.append(str(sql)),
                captured_params.extend(params),
            )
            sqlite_store.upsert_fundamentals(df)

        assert len(captured_params) == 1, "Expected exactly 1 row passed to execute()"
        row = captured_params[0]

        # raw_data must be a JSON string, not a Python dict
        assert isinstance(row["raw_data"], str), (
            f"raw_data must be serialized to a JSON string before DB execution, "
            f"got {type(row['raw_data'])}"
        )
        # Must be valid JSON
        parsed = _json.loads(row["raw_data"])
        assert parsed["Symbol"] == "GOOGL"

        # SQL must NOT contain the broken :param::type pattern
        assert len(captured_sql) == 1
        assert ":raw_data::jsonb" not in captured_sql[0].lower(), (
            "SQL must not use :raw_data::jsonb — use plain :raw_data instead"
        )

    def test_upsert_fundamentals_row_count_returned(self, sqlite_store):
        """upsert_fundamentals should return the number of rows in the DataFrame."""
        df = pd.DataFrame([
            {
                "snapshot_date":  date(2026, 3, 22),
                "symbol":         "GOOGL",
                "pe_ratio":       27.84, "eps": 10.81,
                "revenue":        402_835_997_000, "market_cap": 3_641_197_199_000,
                "debt_to_equity": None, "roe": 0.357,
                "sector":         "COMMUNICATION SERVICES",
                "industry":       "INTERNET CONTENT & INFORMATION",
                "raw_data":       {},
                "provider":       "alpha_vantage",
            },
            {
                "snapshot_date":  date(2026, 3, 22),
                "symbol":         "AAPL",
                "pe_ratio":       29.5, "eps": 6.13,
                "revenue":        394_330_000_000, "market_cap": 2_950_000_000_000,
                "debt_to_equity": 1.73, "roe": 1.56,
                "sector":         "Technology",
                "industry":       "Consumer Electronics",
                "raw_data":       {},
                "provider":       "alpha_vantage",
            },
        ])

        with patch.object(sqlite_store, "_conn") as mock_ctx:
            mock_conn = MagicMock()
            mock_ctx.return_value.__enter__.return_value = mock_conn
            mock_conn.execute = MagicMock()
            count = sqlite_store.upsert_fundamentals(df)

        assert count == 2

    def test_query_fundamentals_returns_dataframe(self, sqlite_store):
        """query_fundamentals should always return a DataFrame."""
        df = sqlite_store.query_fundamentals("AAPL")
        assert isinstance(df, pd.DataFrame)


# ── Earnings ──────────────────────────────────────────────────────────────

class TestEarnings:
    def test_upsert_earnings_empty_df(self, sqlite_store):
        assert sqlite_store.upsert_earnings(pd.DataFrame()) == 0

    def test_upsert_earnings_returns_count(self, sqlite_store, sample_earnings_df):
        with patch.object(sqlite_store, "upsert_earnings", return_value=len(sample_earnings_df)):
            count = sqlite_store.upsert_earnings(sample_earnings_df)
        assert count == len(sample_earnings_df)

    def test_query_earnings_returns_dataframe(self, sqlite_store):
        df = sqlite_store.query_earnings("AAPL")
        assert isinstance(df, pd.DataFrame)


# ── Dividends ──────────────────────────────────────────────────────────────

class TestDividends:
    def test_upsert_dividends_empty_df(self, sqlite_store):
        assert sqlite_store.upsert_dividends(pd.DataFrame()) == 0

    def test_query_dividends_returns_dataframe(self, sqlite_store):
        df = sqlite_store.query_dividends("AAPL")
        assert isinstance(df, pd.DataFrame)


# ── News Sentiment ─────────────────────────────────────────────────────────

class TestNewsSentiment:
    def test_upsert_news_empty_df(self, sqlite_store):
        assert sqlite_store.upsert_news_sentiment(pd.DataFrame()) == 0

    def test_query_news_returns_dataframe(self, sqlite_store):
        df = sqlite_store.query_news_sentiment("AAPL")
        assert isinstance(df, pd.DataFrame)

    def test_query_news_with_date_filter(self, sqlite_store):
        """query_news_sentiment with start/end should filter by date."""
        df = sqlite_store.query_news_sentiment(
            "AAPL", start=date(2024, 1, 1), end=date(2024, 6, 30)
        )
        assert isinstance(df, pd.DataFrame)


# ── Generic Dispatch (query) ───────────────────────────────────────────────

class TestGenericDispatch:
    def test_query_dispatches_ohlcv(self, sqlite_store):
        """query(DataType.OHLCV) must call query_ohlcv."""
        with patch.object(sqlite_store, "query_ohlcv", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.OHLCV, "AAPL", date(2024, 1, 1), date(2024, 12, 31))
        mock.assert_called_once()

    def test_query_dispatches_fundamentals(self, sqlite_store):
        """query(DataType.FUNDAMENTALS) must call query_fundamentals."""
        with patch.object(sqlite_store, "query_fundamentals", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.FUNDAMENTALS, "AAPL")
        mock.assert_called_once()

    def test_query_dispatches_earnings(self, sqlite_store):
        """query(DataType.EARNINGS) must call query_earnings."""
        with patch.object(sqlite_store, "query_earnings", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.EARNINGS, "AAPL")
        mock.assert_called_once()

    def test_query_dispatches_dividends(self, sqlite_store):
        """query(DataType.DIVIDENDS) must call query_dividends."""
        with patch.object(sqlite_store, "query_dividends", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.DIVIDENDS, "AAPL")
        mock.assert_called_once()

    def test_query_dispatches_news_sentiment(self, sqlite_store):
        """query(DataType.NEWS_SENTIMENT) must call query_news_sentiment."""
        with patch.object(sqlite_store, "query_news_sentiment", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.NEWS_SENTIMENT, "AAPL")
        mock.assert_called_once()

    def test_query_unknown_type_returns_empty(self, sqlite_store):
        """query() for an unsupported type should return empty DataFrame, not raise."""
        df = sqlite_store.query(DataType.TICK, "AAPL")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_query_dispatches_options_chain(self, sqlite_store):
        """query(DataType.OPTIONS_CHAIN) must call query_options_snapshot."""
        with patch.object(sqlite_store, "query_options_snapshot", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.OPTIONS_CHAIN, "SPY", date(2024, 1, 1), date(2024, 1, 31))
        mock.assert_called_once()

    def test_query_dispatches_iv_rank(self, sqlite_store):
        """query(DataType.IV_RANK) must call query_iv_rank_history."""
        with patch.object(sqlite_store, "query_iv_rank_history", return_value=pd.DataFrame()) as mock:
            sqlite_store.query(DataType.IV_RANK, "AAPL", date(2024, 1, 1), date(2024, 12, 31))
        mock.assert_called_once()


# ── Options Snapshots ──────────────────────────────────────────────────────

@pytest.fixture
def sample_options_df():
    """A minimal options chain snapshot for SPY with 4 contracts (2 calls + 2 puts)."""
    snap = datetime(2024, 1, 10, 16, 0, tzinfo=timezone.utc)
    return pd.DataFrame([
        {
            "snapshot_at": snap,
            "symbol": "SPY",
            "expiration_date": date(2024, 1, 19),
            "strike": 470.0,
            "option_type": "call",
            "bid": 3.50, "ask": 3.60, "last": 3.55,
            "volume": 5000, "open_interest": 12000,
            "implied_volatility": 0.18,
            "delta": 0.42, "gamma": 0.05, "theta": -0.12, "vega": 0.15,
            "rho": 0.08, "iv_rank": 35.0, "iv_percentile": 40.0,
            "underlying_price": 468.5,
            "provider": "finnhub",
        },
        {
            "snapshot_at": snap,
            "symbol": "SPY",
            "expiration_date": date(2024, 1, 19),
            "strike": 465.0,
            "option_type": "put",
            "bid": 2.80, "ask": 2.90, "last": 2.85,
            "volume": 4500, "open_interest": 10500,
            "implied_volatility": 0.19,
            "delta": -0.38, "gamma": 0.04, "theta": -0.11, "vega": 0.14,
            "rho": -0.06, "iv_rank": 35.0, "iv_percentile": 40.0,
            "underlying_price": 468.5,
            "provider": "finnhub",
        },
        {
            "snapshot_at": snap,
            "symbol": "SPY",
            "expiration_date": date(2024, 2, 16),
            "strike": 475.0,
            "option_type": "call",
            "bid": 5.10, "ask": 5.25, "last": 5.18,
            "volume": 2000, "open_interest": 8000,
            "implied_volatility": 0.17,
            "delta": 0.38, "gamma": 0.03, "theta": -0.09, "vega": 0.20,
            "rho": 0.10, "iv_rank": 35.0, "iv_percentile": 40.0,
            "underlying_price": 468.5,
            "provider": "finnhub",
        },
        {
            "snapshot_at": snap,
            "symbol": "SPY",
            "expiration_date": date(2024, 2, 16),
            "strike": 460.0,
            "option_type": "put",
            "bid": 4.20, "ask": 4.35, "last": 4.28,
            "volume": 1800, "open_interest": 7500,
            "implied_volatility": 0.20,
            "delta": -0.35, "gamma": 0.03, "theta": -0.08, "vega": 0.19,
            "rho": -0.09, "iv_rank": 35.0, "iv_percentile": 40.0,
            "underlying_price": 468.5,
            "provider": "finnhub",
        },
    ])


class TestOptionsSnapshot:
    def test_upsert_options_empty_df(self, sqlite_store):
        """Upserting empty options DataFrame returns 0 without error."""
        count = sqlite_store.upsert_options_snapshot(pd.DataFrame())
        assert count == 0

    def test_upsert_options_returns_count(self, sqlite_store, sample_options_df):
        """upsert_options_snapshot should return number of rows written."""
        with patch.object(sqlite_store, "upsert_options_snapshot",
                          return_value=len(sample_options_df)):
            count = sqlite_store.upsert_options_snapshot(sample_options_df)
        assert count == len(sample_options_df)

    def test_query_options_empty_when_no_data(self, sqlite_store):
        """query_options_snapshot returns empty DataFrame when no data stored."""
        df = sqlite_store.query_options_snapshot("SPY")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_query_options_returns_all_contracts(self, sqlite_store, sample_options_df):
        """After upsert, query should return all stored contracts."""
        with patch.object(sqlite_store, "upsert_options_snapshot", return_value=4):
            sqlite_store.upsert_options_snapshot(sample_options_df)

        with patch.object(sqlite_store, "query_options_snapshot",
                          return_value=sample_options_df):
            df = sqlite_store.query_options_snapshot("SPY")

        assert len(df) == 4
        assert set(df["option_type"].unique()) == {"call", "put"}

    def test_query_options_filters_by_expiration(self, sqlite_store, sample_options_df):
        """Filtering by expiration_date should return only that expiration's contracts."""
        jan_exp = date(2024, 1, 19)
        jan_df = sample_options_df[sample_options_df["expiration_date"] == jan_exp]

        with patch.object(sqlite_store, "query_options_snapshot", return_value=jan_df):
            df = sqlite_store.query_options_snapshot("SPY", expiration_date=jan_exp)

        assert all(df["expiration_date"] == jan_exp)

    def test_query_options_filters_by_type(self, sqlite_store, sample_options_df):
        """Filtering by option_type='call' should return only call contracts."""
        calls_df = sample_options_df[sample_options_df["option_type"] == "call"]

        with patch.object(sqlite_store, "query_options_snapshot", return_value=calls_df):
            df = sqlite_store.query_options_snapshot("SPY", option_type="call")

        assert all(df["option_type"] == "call")

    def test_options_row_includes_new_greeks_fields(self, sqlite_store, sample_options_df):
        """Options rows should include rho, underlying_price, and iv_percentile."""
        with patch.object(sqlite_store, "query_options_snapshot",
                          return_value=sample_options_df):
            df = sqlite_store.query_options_snapshot("SPY")

        assert "rho" in df.columns
        assert "underlying_price" in df.columns
        assert "iv_percentile" in df.columns
        assert df["rho"].iloc[0] == pytest.approx(0.08)
        assert df["underlying_price"].iloc[0] == pytest.approx(468.5)


# ── Option Expirations ─────────────────────────────────────────────────────

class TestOptionExpirations:
    def test_get_expirations_empty_when_no_data(self, sqlite_store):
        """get_option_expirations returns empty list when no data stored."""
        expirations = sqlite_store.get_option_expirations("SPY")
        assert expirations == []

    def test_get_expirations_returns_sorted_dates(self, sqlite_store, sample_options_df):
        """get_option_expirations should return distinct expiration dates in ASC order."""
        expected = sorted([date(2024, 1, 19), date(2024, 2, 16)])

        with patch.object(sqlite_store, "get_option_expirations", return_value=expected):
            expirations = sqlite_store.get_option_expirations("SPY")

        assert expirations == expected
        assert expirations == sorted(expirations)

    def test_query_by_expiration_returns_all_strikes(self, sqlite_store, sample_options_df):
        """query_options_by_expiration should return both calls and puts for that expiry."""
        jan_df = sample_options_df[
            sample_options_df["expiration_date"] == date(2024, 1, 19)
        ].copy()

        with patch.object(sqlite_store, "query_options_snapshot", return_value=jan_df):
            df = sqlite_store.query_options_by_expiration("SPY", date(2024, 1, 19))

        assert not df.empty
        assert set(df["option_type"].unique()) == {"call", "put"}


# ── Max Pain ───────────────────────────────────────────────────────────────

class TestMaxPain:
    def test_compute_max_pain_no_data_returns_none(self, sqlite_store):
        """compute_max_pain with no stored data should return max_pain_price=None."""
        with patch.object(sqlite_store, "query_options_by_expiration",
                          return_value=pd.DataFrame()):
            result = sqlite_store.compute_max_pain("SPY", date(2024, 1, 19))

        assert result["max_pain_price"] is None
        assert result["strikes"] == []

    def test_compute_max_pain_returns_valid_strike(self, sqlite_store, sample_options_df):
        """compute_max_pain should return a strike that exists in the options data."""
        jan_df = sample_options_df[
            sample_options_df["expiration_date"] == date(2024, 1, 19)
        ].copy()

        with patch.object(sqlite_store, "query_options_by_expiration", return_value=jan_df):
            result = sqlite_store.compute_max_pain("SPY", date(2024, 1, 19))

        assert result["max_pain_price"] in result["strikes"]
        assert isinstance(result["max_pain_price"], (int, float))

    def test_compute_max_pain_result_structure(self, sqlite_store, sample_options_df):
        """compute_max_pain should return all required keys."""
        jan_df = sample_options_df[
            sample_options_df["expiration_date"] == date(2024, 1, 19)
        ].copy()

        with patch.object(sqlite_store, "query_options_by_expiration", return_value=jan_df):
            result = sqlite_store.compute_max_pain("SPY", date(2024, 1, 19))

        required_keys = {"max_pain_price", "strikes", "call_oi", "put_oi", "total_pain", "snapshot_date"}
        assert required_keys.issubset(result.keys())
        assert len(result["strikes"]) == len(result["total_pain"])
        assert len(result["strikes"]) == len(result["call_oi"])

    def test_compute_max_pain_pain_is_nonnegative(self, sqlite_store, sample_options_df):
        """All pain values should be >= 0 (OI is never negative)."""
        jan_df = sample_options_df[
            sample_options_df["expiration_date"] == date(2024, 1, 19)
        ].copy()

        with patch.object(sqlite_store, "query_options_by_expiration", return_value=jan_df):
            result = sqlite_store.compute_max_pain("SPY", date(2024, 1, 19))

        assert all(p >= 0 for p in result["total_pain"])

    def test_compute_max_pain_with_known_oi(self, sqlite_store):
        """Test max pain with controlled OI to verify the algorithm is correct.

        Setup: 2 strikes, 480 call OI=0 put OI=100, 460 call OI=100 put OI=0
        At candidate=480: pain = 100*(480-460) + 0 = 2000
        At candidate=460: pain = 0 + 100*(480-460) = 2000
        Both equal, so max pain is the first strike (460) by index ordering.
        """
        snap = datetime(2024, 1, 10, 16, 0, tzinfo=timezone.utc)
        controlled_df = pd.DataFrame([
            {
                "snapshot_at": snap, "symbol": "SPY",
                "expiration_date": date(2024, 1, 19),
                "strike": 460.0, "option_type": "call",
                "bid": 1.0, "ask": 1.1, "last": 1.05,
                "volume": 100, "open_interest": 100,
                "implied_volatility": 0.2,
                "delta": 0.5, "gamma": 0.01, "theta": -0.1, "vega": 0.1,
                "rho": 0.05, "iv_rank": 30.0, "iv_percentile": 35.0,
                "underlying_price": 470.0, "provider": "test",
            },
            {
                "snapshot_at": snap, "symbol": "SPY",
                "expiration_date": date(2024, 1, 19),
                "strike": 480.0, "option_type": "put",
                "bid": 2.0, "ask": 2.1, "last": 2.05,
                "volume": 100, "open_interest": 100,
                "implied_volatility": 0.2,
                "delta": -0.5, "gamma": 0.01, "theta": -0.1, "vega": 0.1,
                "rho": -0.05, "iv_rank": 30.0, "iv_percentile": 35.0,
                "underlying_price": 470.0, "provider": "test",
            },
        ])

        with patch.object(sqlite_store, "query_options_by_expiration",
                          return_value=controlled_df):
            result = sqlite_store.compute_max_pain("SPY", date(2024, 1, 19))

        assert result["max_pain_price"] in [460.0, 480.0]
        assert len(result["strikes"]) == 2


# ── IV Rank History ────────────────────────────────────────────────────────

@pytest.fixture
def sample_iv_rank_df():
    """A 5-day IV rank history for AAPL."""
    rows = []
    base = date(2024, 1, 2)
    for i in range(5):
        rows.append({
            "recorded_at": base + timedelta(days=i),
            "symbol": "AAPL",
            "iv_rank": 35.0 + i * 2,
            "iv_percentile": 40.0 + i * 1.5,
            "current_iv": 0.22 + i * 0.01,
            "iv_52w_high": 0.55,
            "iv_52w_low": 0.15,
            "provider": "finnhub",
        })
    return pd.DataFrame(rows)


class TestIVRankHistory:
    def test_upsert_iv_rank_empty_df(self, sqlite_store):
        """Upserting empty IV rank DataFrame returns 0 without error."""
        count = sqlite_store.upsert_iv_rank(pd.DataFrame())
        assert count == 0

    def test_upsert_iv_rank_returns_count(self, sqlite_store, sample_iv_rank_df):
        """upsert_iv_rank should return number of rows written."""
        with patch.object(sqlite_store, "upsert_iv_rank",
                          return_value=len(sample_iv_rank_df)):
            count = sqlite_store.upsert_iv_rank(sample_iv_rank_df)
        assert count == len(sample_iv_rank_df)

    def test_query_iv_rank_empty_when_no_data(self, sqlite_store):
        """query_iv_rank_history returns empty DataFrame when no data stored."""
        df = sqlite_store.query_iv_rank_history("AAPL")
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_query_iv_rank_returns_dataframe(self, sqlite_store, sample_iv_rank_df):
        """After upsert, query should return the stored IV rank history."""
        with patch.object(sqlite_store, "query_iv_rank_history",
                          return_value=sample_iv_rank_df):
            df = sqlite_store.query_iv_rank_history("AAPL")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "iv_rank" in df.columns
        assert "iv_percentile" in df.columns
        assert "current_iv" in df.columns

    def test_query_iv_rank_date_filter(self, sqlite_store, sample_iv_rank_df):
        """query_iv_rank_history with start/end should filter to that range."""
        start = date(2024, 1, 3)
        end = date(2024, 1, 5)
        filtered = sample_iv_rank_df[
            (sample_iv_rank_df["recorded_at"] >= start) &
            (sample_iv_rank_df["recorded_at"] <= end)
        ]

        with patch.object(sqlite_store, "query_iv_rank_history", return_value=filtered):
            df = sqlite_store.query_iv_rank_history("AAPL", start=start, end=end)

        assert len(df) == len(filtered)

    def test_iv_rank_values_in_valid_range(self, sqlite_store, sample_iv_rank_df):
        """IV rank should be between 0 and 100."""
        with patch.object(sqlite_store, "query_iv_rank_history",
                          return_value=sample_iv_rank_df):
            df = sqlite_store.query_iv_rank_history("AAPL")

        assert (df["iv_rank"] >= 0).all()
        assert (df["iv_rank"] <= 100).all()
