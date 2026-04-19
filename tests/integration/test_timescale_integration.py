"""
tests/integration/test_timescale_integration.py
Integration tests for TimescaleStore against a real TimescaleDB instance.

Tests cover OHLCV, options snapshots, max pain, IV rank history, and the
generic query() dispatcher. All tests use isolated synthetic symbols
("ITEST_*") to avoid colliding with real market data.

Run:
    pytest tests/integration/test_timescale_integration.py -m integration -v

Skip automatically if TimescaleDB is not reachable.
"""

from datetime import date, datetime, timezone, timedelta

import pandas as pd
import pytest

from market_data.storage.timescale import TimescaleStore


# ── Helpers ────────────────────────────────────────────────────────────────

def _store_reachable() -> bool:
    try:
        return TimescaleStore().ping()
    except Exception:
        return False


skip_no_db = pytest.mark.skipif(
    not _store_reachable(),
    reason="TimescaleDB not reachable — run: docker-compose up -d timescaledb"
)


def _make_options_df(
    symbol: str = "ITEST",
    snapshot_date: date = date(2024, 6, 1),
    expiration_date: date = date(2024, 6, 21),
    strikes: list[float] | None = None,
) -> pd.DataFrame:
    """
    Build a small synthetic options chain DataFrame for testing.
    Generates call + put rows for each strike.
    """
    if strikes is None:
        strikes = [490.0, 495.0, 500.0, 505.0, 510.0]

    snapshot_at = datetime(
        snapshot_date.year, snapshot_date.month, snapshot_date.day,
        tzinfo=timezone.utc,
    )
    rows = []
    for strike in strikes:
        for opt_type, delta_sign in [("call", 1), ("put", -1)]:
            rows.append({
                "snapshot_at":        snapshot_at,
                "symbol":             symbol,
                "expiration_date":    expiration_date,
                "strike":             strike,
                "option_type":        opt_type,
                "bid":                1.00,
                "ask":                1.05,
                "last":               1.02,
                "volume":             100 + int(strike),
                "open_interest":      1000 + int(strike) * 2,
                "implied_volatility": 0.20,
                "delta":              delta_sign * 0.50,
                "gamma":              0.05,
                "theta":              -0.10,
                "vega":               0.15,
                "rho":                0.01,
                "iv_rank":            55.0,
                "iv_percentile":      60.0,
                "underlying_price":   500.0,
                "provider":           "test",
            })
    return pd.DataFrame(rows)


def _make_iv_rank_df(
    symbol: str = "ITEST",
    days: int = 10,
    base_date: date = date(2024, 1, 1),
) -> pd.DataFrame:
    rows = []
    for i in range(days):
        d = base_date + timedelta(days=i)
        rows.append({
            "recorded_at":  d,
            "symbol":       symbol,
            "iv_rank":      30.0 + i,
            "iv_percentile": 35.0 + i,
            "current_iv":   0.18 + i * 0.005,
            "iv_52w_high":  0.45,
            "iv_52w_low":   0.12,
            "provider":     "test",
        })
    return pd.DataFrame(rows)


# ── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def store():
    """Real TimescaleStore for the duration of the module."""
    return TimescaleStore()


# ── Ping ──────────────────────────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestPing:
    def test_ping_returns_true(self, store):
        assert store.ping() is True


# ── OHLCV ─────────────────────────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestOHLCVRoundtrip:
    SYMBOL = "ITEST_OHLCV"

    def _make_df(self, days: int = 5) -> pd.DataFrame:
        base = datetime(2024, 3, 1, tzinfo=timezone.utc)
        rows = []
        for i in range(days):
            rows.append({
                "timestamp": base + timedelta(days=i),
                "symbol":    self.SYMBOL,
                "open":      100.0 + i,
                "high":      105.0 + i,
                "low":       99.0 + i,
                "close":     103.0 + i,
                "volume":    1_000_000 + i * 10_000,
                "adj_close": 103.0 + i,
                "provider":  "test",
            })
        return pd.DataFrame(rows)

    def test_upsert_and_query(self, store):
        df = self._make_df()
        written = store.upsert_ohlcv(df)
        assert written == 5

        result = store.query_ohlcv(
            self.SYMBOL,
            start=date(2024, 3, 1),
            end=date(2024, 3, 5),
        )
        assert not result.empty
        assert len(result) == 5
        assert set(["timestamp", "symbol", "open", "high", "low", "close", "volume"]).issubset(result.columns)

    def test_upsert_is_idempotent(self, store):
        df = self._make_df()
        store.upsert_ohlcv(df)
        written_again = store.upsert_ohlcv(df)
        assert written_again == 5  # idempotent — same count, no error

    def test_query_returns_empty_for_missing_symbol(self, store):
        result = store.query_ohlcv(
            "ITEST_DOES_NOT_EXIST",
            start=date(2024, 3, 1),
            end=date(2024, 3, 5),
        )
        assert result.empty


# ── Options Snapshots ──────────────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestOptionsSnapshotRoundtrip:
    SYMBOL = "ITEST_OPT"
    SNAP_DATE = date(2024, 6, 1)
    EXP_DATE  = date(2024, 6, 21)

    @pytest.fixture(autouse=True)
    def seed_options(self, store):
        """Insert synthetic options chain once for all tests in this class."""
        df = _make_options_df(self.SYMBOL, self.SNAP_DATE, self.EXP_DATE)
        store.upsert_options_snapshot(df)

    def test_upsert_returns_correct_row_count(self, store):
        df = _make_options_df(self.SYMBOL, self.SNAP_DATE, self.EXP_DATE)
        written = store.upsert_options_snapshot(df)
        assert written == len(df)  # 5 strikes × 2 types = 10

    def test_query_latest_snapshot_returns_data(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        assert not result.empty
        assert (result["symbol"] == self.SYMBOL).all()

    def test_query_filtered_by_expiration(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            expiration_date=self.EXP_DATE,
        )
        assert not result.empty
        assert (result["expiration_date"].astype(str) == str(self.EXP_DATE)).all()

    def test_query_filtered_by_option_type_call(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            expiration_date=self.EXP_DATE,
            option_type="call",
        )
        assert not result.empty
        assert (result["option_type"] == "call").all()

    def test_query_filtered_by_option_type_put(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            expiration_date=self.EXP_DATE,
            option_type="put",
        )
        assert not result.empty
        assert (result["option_type"] == "put").all()

    def test_query_sorted_by_strike(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            expiration_date=self.EXP_DATE,
            option_type="call",
        )
        strikes = result["strike"].tolist()
        assert strikes == sorted(strikes)

    def test_query_nonexistent_snapshot_date_returns_empty(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            snapshot_date=date(1990, 1, 1),
        )
        assert result.empty

    def test_upsert_is_idempotent(self, store):
        df = _make_options_df(self.SYMBOL, self.SNAP_DATE, self.EXP_DATE)
        store.upsert_options_snapshot(df)
        # Second upsert should succeed (ON CONFLICT DO UPDATE)
        written = store.upsert_options_snapshot(df)
        assert written == len(df)

    def test_schema_columns_present(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        expected = {
            "symbol", "strike", "option_type", "expiration_date",
            "bid", "ask", "last", "volume", "open_interest",
            "implied_volatility", "delta", "gamma", "theta", "vega",
            "rho", "iv_rank", "iv_percentile", "underlying_price", "provider",
        }
        assert expected.issubset(set(result.columns))

    def test_underlying_price_preserved(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            expiration_date=self.EXP_DATE,
            option_type="call",
        )
        assert (result["underlying_price"] == 500.0).all()

    def test_open_interest_preserved(self, store):
        result = store.query_options_snapshot(
            self.SYMBOL,
            expiration_date=self.EXP_DATE,
        )
        assert result["open_interest"].notna().all()

    def test_provider_tag_preserved(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        assert (result["provider"] == "test").all()


# ── Options Expirations ────────────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestGetOptionExpirations:
    SYMBOL = "ITEST_EXP"

    @pytest.fixture(autouse=True)
    def seed_two_expirations(self, store):
        exp1 = date(2024, 7, 19)
        exp2 = date(2024, 8, 16)
        snap = date(2024, 7, 1)
        df1 = _make_options_df(self.SYMBOL, snap, exp1)
        df2 = _make_options_df(self.SYMBOL, snap, exp2)
        store.upsert_options_snapshot(pd.concat([df1, df2], ignore_index=True))

    def test_returns_sorted_expiration_dates(self, store):
        expirations = store.get_option_expirations(self.SYMBOL)
        assert len(expirations) >= 2
        assert expirations == sorted(expirations)

    def test_returns_correct_dates(self, store):
        expirations = store.get_option_expirations(self.SYMBOL)
        exp_strs = [str(e) for e in expirations]
        assert "2024-07-19" in exp_strs
        assert "2024-08-16" in exp_strs

    def test_returns_empty_for_unknown_symbol(self, store):
        expirations = store.get_option_expirations("ITEST_UNKNOWN_SYMBOL_XYZ")
        assert expirations == []


# ── query_options_by_expiration ────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestQueryByExpiration:
    SYMBOL = "ITEST_BYEXP"
    SNAP_DATE = date(2024, 6, 1)
    EXP_DATE  = date(2024, 6, 21)
    STRIKES   = [490.0, 495.0, 500.0, 505.0, 510.0]

    @pytest.fixture(autouse=True)
    def seed(self, store):
        df = _make_options_df(self.SYMBOL, self.SNAP_DATE, self.EXP_DATE, self.STRIKES)
        store.upsert_options_snapshot(df)

    def test_returns_all_strikes_for_expiration(self, store):
        result = store.query_options_by_expiration(self.SYMBOL, self.EXP_DATE)
        assert not result.empty
        assert set(result["strike"].unique()) == set(self.STRIKES)

    def test_returns_both_call_and_put(self, store):
        result = store.query_options_by_expiration(self.SYMBOL, self.EXP_DATE)
        assert set(result["option_type"].unique()) == {"call", "put"}

    def test_row_count_is_strikes_times_two(self, store):
        result = store.query_options_by_expiration(self.SYMBOL, self.EXP_DATE)
        assert len(result) == len(self.STRIKES) * 2


# ── Max Pain ───────────────────────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestComputeMaxPain:
    SYMBOL = "ITEST_MAXPAIN"
    SNAP_DATE = date(2024, 6, 1)
    EXP_DATE  = date(2024, 6, 21)
    # Skew OI so the true max pain is at 500
    STRIKES = [490.0, 495.0, 500.0, 505.0, 510.0]

    @pytest.fixture(autouse=True)
    def seed_skewed_chain(self, store):
        """
        Seed with skewed OI so the max pain calculation has a deterministic result.
        Heavy call OI above 500, heavy put OI below 500 → max pain lands near 500.
        """
        snapshot_at = datetime(2024, 6, 1, tzinfo=timezone.utc)
        rows = []
        for strike in self.STRIKES:
            # Calls: heavy OI above ATM
            rows.append({
                "snapshot_at": snapshot_at, "symbol": self.SYMBOL,
                "expiration_date": self.EXP_DATE,
                "strike": strike, "option_type": "call",
                "bid": None, "ask": None, "last": 1.0,
                "volume": 100,
                "open_interest": 5000 if strike > 500 else 100,
                "implied_volatility": 0.20,
                "delta": None, "gamma": None, "theta": None,
                "vega": None, "rho": None, "iv_rank": None,
                "iv_percentile": None, "underlying_price": 500.0,
                "provider": "test",
            })
            # Puts: heavy OI below ATM
            rows.append({
                "snapshot_at": snapshot_at, "symbol": self.SYMBOL,
                "expiration_date": self.EXP_DATE,
                "strike": strike, "option_type": "put",
                "bid": None, "ask": None, "last": 1.0,
                "volume": 100,
                "open_interest": 5000 if strike < 500 else 100,
                "implied_volatility": 0.20,
                "delta": None, "gamma": None, "theta": None,
                "vega": None, "rho": None, "iv_rank": None,
                "iv_percentile": None, "underlying_price": 500.0,
                "provider": "test",
            })
        store.upsert_options_snapshot(pd.DataFrame(rows))

    def test_returns_dict_with_max_pain_price(self, store):
        result = store.compute_max_pain(self.SYMBOL, self.EXP_DATE)
        assert "max_pain_price" in result
        assert result["max_pain_price"] is not None

    def test_max_pain_is_a_valid_strike(self, store):
        result = store.compute_max_pain(self.SYMBOL, self.EXP_DATE)
        assert result["max_pain_price"] in self.STRIKES

    def test_max_pain_near_atm_with_symmetric_skew(self, store):
        """With heavy call OI above ATM and put OI below ATM, max pain should be ATM."""
        result = store.compute_max_pain(self.SYMBOL, self.EXP_DATE)
        assert result["max_pain_price"] == 500.0

    def test_returns_all_arrays(self, store):
        result = store.compute_max_pain(self.SYMBOL, self.EXP_DATE)
        assert len(result["strikes"]) == len(self.STRIKES)
        assert len(result["call_oi"]) == len(self.STRIKES)
        assert len(result["put_oi"]) == len(self.STRIKES)
        assert len(result["total_pain"]) == len(self.STRIKES)

    def test_returns_none_for_missing_symbol(self, store):
        result = store.compute_max_pain("ITEST_MISSING_XYZ", self.EXP_DATE)
        assert result["max_pain_price"] is None
        assert result["strikes"] == []


# ── IV Rank History ────────────────────────────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestIVRankHistory:
    SYMBOL = "ITEST_IV"
    BASE_DATE = date(2024, 1, 1)
    DAYS = 10

    @pytest.fixture(autouse=True)
    def seed(self, store):
        df = _make_iv_rank_df(self.SYMBOL, self.DAYS, self.BASE_DATE)
        store.upsert_iv_rank(df)

    def test_upsert_returns_correct_row_count(self, store):
        df = _make_iv_rank_df(self.SYMBOL, self.DAYS, self.BASE_DATE)
        written = store.upsert_iv_rank(df)
        assert written == self.DAYS

    def test_query_returns_all_rows_no_filter(self, store):
        result = store.query_iv_rank_history(self.SYMBOL)
        assert len(result) >= self.DAYS

    def test_query_date_range_filter(self, store):
        start = self.BASE_DATE + timedelta(days=2)
        end   = self.BASE_DATE + timedelta(days=5)
        result = store.query_iv_rank_history(self.SYMBOL, start=start, end=end)
        assert not result.empty
        assert len(result) == 4  # days 2, 3, 4, 5

    def test_iv_rank_values_are_correct(self, store):
        result = store.query_iv_rank_history(
            self.SYMBOL,
            start=self.BASE_DATE,
            end=self.BASE_DATE,
        )
        assert not result.empty
        assert float(result.iloc[0]["iv_rank"]) == pytest.approx(30.0)

    def test_upsert_is_idempotent(self, store):
        df = _make_iv_rank_df(self.SYMBOL, self.DAYS, self.BASE_DATE)
        store.upsert_iv_rank(df)
        written = store.upsert_iv_rank(df)
        assert written == self.DAYS

    def test_returns_empty_for_unknown_symbol(self, store):
        result = store.query_iv_rank_history("ITEST_IV_UNKNOWN_XYZ")
        assert result.empty


# ── Databento-style Options (NULL Greeks) ─────────────────────────────────

@pytest.mark.integration
@skip_no_db
class TestDatabentoStyleOptions:
    """
    Verify that options rows with NULL Greeks (as imported from Databento OHLCV-1D)
    can be written and read back cleanly — matching the import script's output.
    """
    SYMBOL = "ITEST_DB"
    SNAP_DATE = date(2024, 3, 8)
    EXP_DATE  = date(2024, 4, 19)

    @pytest.fixture(autouse=True)
    def seed_null_greeks(self, store):
        snapshot_at = datetime(2024, 3, 8, tzinfo=timezone.utc)
        rows = []
        for strike in [500.0, 505.0, 510.0]:
            for opt_type in ["call", "put"]:
                rows.append({
                    "snapshot_at":        snapshot_at,
                    "symbol":             self.SYMBOL,
                    "expiration_date":    self.EXP_DATE,
                    "strike":             strike,
                    "option_type":        opt_type,
                    "bid":                None,
                    "ask":                None,
                    "last":               2.50,
                    "volume":             1500,
                    "open_interest":      None,
                    "implied_volatility": None,
                    "delta":              None,
                    "gamma":              None,
                    "theta":              None,
                    "vega":               None,
                    "rho":                None,
                    "iv_rank":            None,
                    "iv_percentile":      None,
                    "underlying_price":   None,
                    "provider":           "databento",
                })
        store.upsert_options_snapshot(pd.DataFrame(rows))

    def test_null_greeks_accepted_no_error(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        assert not result.empty

    def test_provider_is_databento(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        assert (result["provider"] == "databento").all()

    def test_last_price_preserved(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        assert (result["last"].sub(2.50).abs() < 1e-6).all()

    def test_null_greeks_round_trip(self, store):
        result = store.query_options_snapshot(self.SYMBOL)
        for col in ["delta", "gamma", "theta", "vega", "rho", "implied_volatility"]:
            assert result[col].isna().all(), f"Expected {col} to be NULL but got values"
