"""
market_data/storage/timescale.py
TimescaleDB read/write client — primary structured data store.

All writes are upsert-safe (idempotent). All queries return pandas DataFrames
with typed columns for direct use in pandas-based strategy code.

Options-specific methods support the full workflow for 2-10 day strategies:
  - query_options_snapshot(): retrieve full chain or filtered by expiration
  - get_option_expirations(): list available DTE dates for a symbol
  - query_options_by_expiration(): all strikes for a specific expiration
  - compute_max_pain(): find the max-pain strike from open interest
  - upsert_iv_rank() / query_iv_rank_history(): daily IV rank tracking
"""

import json
import logging
from contextlib import contextmanager
from datetime import date, datetime
from typing import Optional, Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from market_data.config import settings
from market_data.models import DataType, Interval

logger = logging.getLogger(__name__)


class TimescaleStore:
    """
    TimescaleDB read/write client.
    Uses SQLAlchemy + raw SQL for maximum control over hypertable operations.

    The connection pool is lazy — no DB connection is made until the first query.
    All methods are safe to call with an empty DataFrame (they return 0 rows written).
    """

    def __init__(self, url: Optional[str] = None):
        self._url = url or settings.timescale_url
        self._engine: Optional[Engine] = None

    def _get_engine(self) -> Engine:
        """Lazily create the connection pool."""
        if self._engine is None:
            self._engine = create_engine(
                self._url,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
            )
        return self._engine

    @contextmanager
    def _conn(self):
        """Context manager that yields a transactional connection."""
        engine = self._get_engine()
        with engine.connect() as conn:
            try:
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ── OHLCV ──────────────────────────────────────────────────────────────

    def upsert_ohlcv(self, df: pd.DataFrame) -> int:
        """
        Upsert OHLCV rows into ohlcv_daily (or ohlcv_intraday based on interval).
        Returns number of rows written.
        Idempotent: re-running with the same data is safe.
        """
        if df.empty:
            return 0

        required = {"timestamp", "symbol", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            raise ValueError(f"Missing OHLCV columns: {missing}")

        rows = df.to_dict("records")
        upsert_sql = text("""
            INSERT INTO ohlcv_daily
                (timestamp, symbol, open, high, low, close, volume, adj_close, provider)
            VALUES
                (:timestamp, :symbol, :open, :high, :low, :close, :volume, :adj_close, :provider)
            ON CONFLICT (timestamp, symbol) DO UPDATE SET
                open      = EXCLUDED.open,
                high      = EXCLUDED.high,
                low       = EXCLUDED.low,
                close     = EXCLUDED.close,
                volume    = EXCLUDED.volume,
                adj_close = EXCLUDED.adj_close,
                provider  = EXCLUDED.provider,
                fetched_at = NOW()
        """)

        with self._conn() as conn:
            conn.execute(upsert_sql, rows)

        logger.debug(f"Upserted {len(rows)} OHLCV rows for {df['symbol'].iloc[0]}")
        return len(rows)

    def query_ohlcv(
        self,
        symbol: str,
        start: date,
        end: date,
        interval: Interval = Interval.ONE_DAY,
    ) -> pd.DataFrame:
        """
        Query OHLCV from TimescaleDB for a symbol and date range.

        Selects ohlcv_intraday for sub-daily intervals, ohlcv_daily otherwise.
        Returns an empty DataFrame if no rows match.
        """
        table = "ohlcv_intraday" if interval != Interval.ONE_DAY else "ohlcv_daily"
        sql = text(f"""
            SELECT timestamp, symbol, open, high, low, close, volume, adj_close, provider
            FROM {table}
            WHERE symbol = :symbol
              AND timestamp >= :start
              AND timestamp <= :end
            ORDER BY timestamp ASC
        """)
        with self._conn() as conn:
            result = conn.execute(sql, {
                "symbol": symbol.upper(),
                "start":  datetime.combine(start, datetime.min.time()),
                "end":    datetime.combine(end, datetime.max.time()),
            })
            rows = result.fetchall()
            columns = result.keys()

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows, columns=list(columns))

    # ── Fundamentals ───────────────────────────────────────────────────────

    def upsert_fundamentals(self, df: pd.DataFrame) -> int:
        """Upsert fundamentals snapshot. Returns rows written."""
        if df.empty:
            return 0
        rows = df.to_dict("records")
        # Serialize raw_data dict to a JSON string.
        # Note: do NOT use :raw_data::jsonb — the PostgreSQL :: cast operator
        # immediately after a SQLAlchemy :param_name causes the parameter to be
        # dropped from substitution, producing a psycopg2 SyntaxError.
        for row in rows:
            if "raw_data" in row and isinstance(row["raw_data"], dict):
                row["raw_data"] = json.dumps(row["raw_data"])
        sql = text("""
            INSERT INTO fundamentals
                (snapshot_date, symbol, pe_ratio, eps, revenue, market_cap,
                 debt_to_equity, roe, sector, industry, raw_data, provider)
            VALUES
                (:snapshot_date, :symbol, :pe_ratio, :eps, :revenue, :market_cap,
                 :debt_to_equity, :roe, :sector, :industry, :raw_data, :provider)
            ON CONFLICT (snapshot_date, symbol) DO UPDATE SET
                pe_ratio = EXCLUDED.pe_ratio, eps = EXCLUDED.eps,
                revenue = EXCLUDED.revenue, market_cap = EXCLUDED.market_cap,
                sector = EXCLUDED.sector, provider = EXCLUDED.provider,
                fetched_at = NOW()
        """)
        with self._conn() as conn:
            conn.execute(sql, rows)
        return len(rows)

    def query_fundamentals(self, symbol: str, latest_only: bool = True) -> pd.DataFrame:
        """Query fundamentals for a symbol. Returns latest record by default."""
        sql = text("""
            SELECT * FROM fundamentals
            WHERE symbol = :symbol
            ORDER BY snapshot_date DESC
            LIMIT :limit
        """)
        with self._conn() as conn:
            result = conn.execute(sql, {"symbol": symbol.upper(), "limit": 1 if latest_only else 100})
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=list(columns)) if rows else pd.DataFrame()

    # ── Earnings ───────────────────────────────────────────────────────────

    def upsert_earnings(self, df: pd.DataFrame) -> int:
        """Upsert earnings rows. Returns rows written."""
        if df.empty:
            return 0
        # Providers (e.g. Alpha Vantage) may omit revenue columns entirely.
        # SQLAlchemy requires every bind parameter key to be present in the dict.
        optional = ("revenue_actual", "revenue_estimate", "fiscal_quarter", "fiscal_year")
        rows = [
            {**{k: None for k in optional}, **r}
            for r in df.to_dict("records")
        ]
        sql = text("""
            INSERT INTO earnings
                (report_date, symbol, eps_actual, eps_estimate, eps_surprise,
                 revenue_actual, revenue_estimate, fiscal_quarter, fiscal_year, provider)
            VALUES
                (:report_date, :symbol, :eps_actual, :eps_estimate, :eps_surprise,
                 :revenue_actual, :revenue_estimate, :fiscal_quarter, :fiscal_year, :provider)
            ON CONFLICT (report_date, symbol) DO UPDATE SET
                eps_actual = EXCLUDED.eps_actual, eps_estimate = EXCLUDED.eps_estimate,
                provider = EXCLUDED.provider, fetched_at = NOW()
        """)
        with self._conn() as conn:
            conn.execute(sql, rows)
        return len(rows)

    def query_earnings(self, symbol: str) -> pd.DataFrame:
        """Query earnings history for a symbol, newest first."""
        sql = text("""
            SELECT report_date, symbol, eps_actual, eps_estimate, eps_surprise,
                   revenue_actual, revenue_estimate, fiscal_quarter, fiscal_year, provider
            FROM earnings
            WHERE symbol = :symbol
            ORDER BY report_date DESC
        """)
        with self._conn() as conn:
            result = conn.execute(sql, {"symbol": symbol.upper()})
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=list(columns)) if rows else pd.DataFrame()

    # ── Dividends ──────────────────────────────────────────────────────────

    def upsert_dividends(self, df: pd.DataFrame) -> int:
        """Upsert dividend rows. Returns rows written."""
        if df.empty:
            return 0
        rows = df.to_dict("records")
        sql = text("""
            INSERT INTO dividends
                (ex_date, symbol, amount, pay_date, declaration_date, provider)
            VALUES
                (:ex_date, :symbol, :amount, :pay_date, :declaration_date, :provider)
            ON CONFLICT (ex_date, symbol) DO UPDATE SET
                amount = EXCLUDED.amount,
                pay_date = EXCLUDED.pay_date,
                declaration_date = EXCLUDED.declaration_date,
                provider = EXCLUDED.provider,
                fetched_at = NOW()
        """)
        with self._conn() as conn:
            conn.execute(sql, rows)
        return len(rows)

    def query_dividends(self, symbol: str) -> pd.DataFrame:
        """Query dividend history for a symbol, newest ex-date first."""
        sql = text("""
            SELECT ex_date, symbol, amount, pay_date, declaration_date, provider
            FROM dividends
            WHERE symbol = :symbol
            ORDER BY ex_date DESC
        """)
        with self._conn() as conn:
            result = conn.execute(sql, {"symbol": symbol.upper()})
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=list(columns)) if rows else pd.DataFrame()

    # ── News Sentiment ─────────────────────────────────────────────────────

    def upsert_news_sentiment(self, df: pd.DataFrame) -> int:
        """Upsert news sentiment rows. Returns rows written."""
        if df.empty:
            return 0
        rows = df.to_dict("records")
        sql = text("""
            INSERT INTO news_sentiment
                (published_at, symbol, headline, source, sentiment_score, sentiment_label, url, provider)
            VALUES
                (:published_at, :symbol, :headline, :source, :sentiment_score, :sentiment_label, :url, :provider)
            ON CONFLICT (published_at, symbol, headline) DO UPDATE SET
                sentiment_score = EXCLUDED.sentiment_score,
                sentiment_label = EXCLUDED.sentiment_label,
                provider = EXCLUDED.provider,
                fetched_at = NOW()
        """)
        with self._conn() as conn:
            conn.execute(sql, rows)
        return len(rows)

    def query_news_sentiment(
        self,
        symbol: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> pd.DataFrame:
        """Query news sentiment for a symbol, optionally filtered by date range."""
        sql = text("""
            SELECT published_at, symbol, headline, source, sentiment_score, sentiment_label, url, provider
            FROM news_sentiment
            WHERE symbol = :symbol
              AND (:start IS NULL OR published_at >= :start)
              AND (:end IS NULL OR published_at <= :end)
            ORDER BY published_at DESC
        """)
        start_dt = datetime.combine(start, datetime.min.time()) if start else None
        end_dt = datetime.combine(end, datetime.max.time()) if end else None
        with self._conn() as conn:
            result = conn.execute(sql, {"symbol": symbol.upper(), "start": start_dt, "end": end_dt})
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=list(columns)) if rows else pd.DataFrame()

    # ── Options Snapshots ──────────────────────────────────────────────────

    def upsert_options_snapshot(self, df: pd.DataFrame) -> int:
        """
        Upsert options chain snapshot rows.

        Stores all contract details including Greeks and IV.
        Idempotent on (snapshot_at, symbol, expiration_date, strike, option_type).
        Returns rows written.
        """
        if df.empty:
            return 0
        rows = df.to_dict("records")
        # Sanitize integer columns after dict conversion: pandas stores int/None as
        # float64 in the DataFrame so to_dict gives floats/NaN. Normalize here so
        # psycopg2 sends proper Python ints (or NULL) to the bigint columns.
        _int_cols = {"volume", "open_interest"}
        for row in rows:
            for col in _int_cols:
                if col in row:
                    v = row[col]
                    if v is None or (isinstance(v, float) and (v != v)):  # None or NaN
                        row[col] = None
                    else:
                        try:
                            row[col] = int(v)
                        except (ValueError, OverflowError):
                            row[col] = None
        sql = text("""
            INSERT INTO options_snapshots
                (snapshot_at, symbol, expiration_date, strike, option_type,
                 bid, ask, last, volume, open_interest, implied_volatility,
                 delta, gamma, theta, vega, rho, iv_rank, iv_percentile,
                 underlying_price, provider)
            VALUES
                (:snapshot_at, :symbol, :expiration_date, :strike, :option_type,
                 :bid, :ask, :last, :volume, :open_interest, :implied_volatility,
                 :delta, :gamma, :theta, :vega, :rho, :iv_rank, :iv_percentile,
                 :underlying_price, :provider)
            ON CONFLICT (snapshot_at, symbol, expiration_date, strike, option_type) DO UPDATE SET
                bid = EXCLUDED.bid, ask = EXCLUDED.ask, last = EXCLUDED.last,
                volume = EXCLUDED.volume, open_interest = EXCLUDED.open_interest,
                implied_volatility = EXCLUDED.implied_volatility,
                delta = EXCLUDED.delta, gamma = EXCLUDED.gamma,
                theta = EXCLUDED.theta, vega = EXCLUDED.vega,
                rho = EXCLUDED.rho,
                iv_rank = EXCLUDED.iv_rank, iv_percentile = EXCLUDED.iv_percentile,
                underlying_price = EXCLUDED.underlying_price,
                provider = EXCLUDED.provider,
                fetched_at = NOW()
        """)
        with self._conn() as conn:
            conn.execute(sql, rows)
        return len(rows)

    def query_options_snapshot(
        self,
        symbol: str,
        snapshot_date: Optional[date] = None,
        expiration_date: Optional[date] = None,
        option_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Query stored options chain for a symbol.

        Args:
            symbol: Ticker symbol (case-insensitive).
            snapshot_date: Query data from this specific snapshot date. If None,
                returns the latest available snapshot.
            expiration_date: Filter to a specific option expiration. If None,
                returns all expirations.
            option_type: Filter to 'call' or 'put'. If None, returns both.

        Returns:
            DataFrame with all matching option rows, sorted by expiration and strike.
            Empty DataFrame if no data is stored.

        Usage by strategy agents:
            # Get latest full chain for SPY
            df = store.query_options_snapshot("SPY")

            # Get only calls expiring Jan 19 2024
            df = store.query_options_snapshot("SPY",
                expiration_date=date(2024, 1, 19), option_type="call")
        """
        # If no snapshot_date given, use the latest available
        if snapshot_date is None:
            with self._conn() as conn:
                result = conn.execute(
                    text("SELECT MAX(DATE(snapshot_at)) FROM options_snapshots WHERE symbol = :sym"),
                    {"sym": symbol.upper()},
                )
                row = result.fetchone()
                if row and row[0]:
                    snapshot_date = date.fromisoformat(str(row[0]))
                else:
                    return pd.DataFrame()

        conditions = [
            "symbol = :symbol",
            "DATE(snapshot_at) = :snapshot_date",
        ]
        params: dict[str, Any] = {
            "symbol": symbol.upper(),
            "snapshot_date": snapshot_date.isoformat(),
        }

        if expiration_date is not None:
            conditions.append("expiration_date = :expiration_date")
            params["expiration_date"] = expiration_date.isoformat()

        if option_type is not None:
            conditions.append("option_type = :option_type")
            params["option_type"] = option_type.lower()

        sql = text(f"""
            SELECT snapshot_at, symbol, expiration_date, strike, option_type,
                   bid, ask, last, volume, open_interest, implied_volatility,
                   delta, gamma, theta, vega, rho, iv_rank, iv_percentile,
                   underlying_price, provider
            FROM options_snapshots
            WHERE {" AND ".join(conditions)}
            ORDER BY expiration_date ASC, strike ASC, option_type ASC
        """)

        with self._conn() as conn:
            result = conn.execute(sql, params)
            rows = result.fetchall()
            columns = result.keys()

        return pd.DataFrame(rows, columns=list(columns)) if rows else pd.DataFrame()

    def get_option_expirations(self, symbol: str) -> list[date]:
        """
        Return all distinct expiration dates available for a symbol.

        Useful for strategy agents to enumerate DTE options before selecting
        the target expiration for a spread or straddle.

        Returns: Sorted list of expiration dates (ascending).
        """
        sql = text("""
            SELECT DISTINCT expiration_date
            FROM options_snapshots
            WHERE symbol = :symbol
            ORDER BY expiration_date ASC
        """)
        with self._conn() as conn:
            result = conn.execute(sql, {"symbol": symbol.upper()})
            rows = result.fetchall()

        return [date.fromisoformat(str(r[0])) for r in rows]

    def query_options_by_expiration(
        self,
        symbol: str,
        expiration_date: date,
        snapshot_date: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Return all strikes (calls and puts) for a specific expiration.

        This is the primary method for building spreads, iron condors, and
        strangles — strategy agents need all strikes for a given expiration
        to compute P&L profiles and select strikes.

        Args:
            symbol: Ticker symbol.
            expiration_date: The option expiration to query.
            snapshot_date: Use data from this date's snapshot. Defaults to latest.

        Returns:
            DataFrame with all contracts for this expiration, sorted by strike.
        """
        return self.query_options_snapshot(
            symbol=symbol,
            snapshot_date=snapshot_date,
            expiration_date=expiration_date,
            option_type=None,
        )

    def compute_max_pain(
        self,
        symbol: str,
        expiration_date: date,
        snapshot_date: Optional[date] = None,
    ) -> dict:
        """
        Compute the max pain strike for a given expiration.

        Max pain theory: option writers (who are net short premium) profit most
        at the strike where total monetary loss to option BUYERS is maximized.
        This is the price where the sum of all in-the-money option values
        (both calls and puts) across all strikes is minimized.

        Formula for each candidate strike S:
            total_pain(S) = sum over all strikes K of:
                call_OI(K) * max(0, K - S) +    # in-the-money puts if S < K
                put_OI(K)  * max(0, S - K)       # in-the-money calls if S > K
        Wait — standard formula (perspective of holders losing money):
            pain(S) = Σ_K [ call_OI(K) * max(0, S - K) + put_OI(K) * max(0, K - S) ]

        Max pain = argmin_S pain(S)

        Returns:
            {
                "max_pain_price": float,            # the max pain strike
                "strikes": [float, ...],            # all evaluated strikes
                "call_oi": [int, ...],              # call open interest per strike
                "put_oi": [int, ...],               # put open interest per strike
                "total_pain": [float, ...],         # pain value at each strike
                "snapshot_date": str,               # date of snapshot used
            }
            Returns {"max_pain_price": None, "strikes": []} if no data found.
        """
        df = self.query_options_by_expiration(symbol, expiration_date, snapshot_date)

        if df.empty or "open_interest" not in df.columns or "strike" not in df.columns:
            return {"max_pain_price": None, "strikes": []}

        # Fill missing OI with 0
        df = df.copy()
        df["open_interest"] = pd.to_numeric(df["open_interest"], errors="coerce").fillna(0)

        calls = df[df["option_type"] == "call"].set_index("strike")["open_interest"]
        puts = df[df["option_type"] == "put"].set_index("strike")["open_interest"]

        all_strikes = sorted(df["strike"].unique().tolist())

        total_pain = []
        call_oi_by_strike = []
        put_oi_by_strike = []

        for candidate in all_strikes:
            pain = 0.0
            for k in all_strikes:
                call_oi = float(calls.get(k, 0))
                put_oi = float(puts.get(k, 0))
                pain += call_oi * max(0, candidate - k)  # call holders lose if candidate < k
                pain += put_oi * max(0, k - candidate)   # put holders lose if candidate > k
            total_pain.append(pain)
            call_oi_by_strike.append(int(calls.get(candidate, 0)))
            put_oi_by_strike.append(int(puts.get(candidate, 0)))

        min_pain_idx = total_pain.index(min(total_pain))
        max_pain_price = all_strikes[min_pain_idx]

        used_snapshot = snapshot_date or date.today()

        return {
            "max_pain_price": max_pain_price,
            "strikes": all_strikes,
            "call_oi": call_oi_by_strike,
            "put_oi": put_oi_by_strike,
            "total_pain": total_pain,
            "snapshot_date": str(used_snapshot),
        }

    # ── IV Rank History ────────────────────────────────────────────────────

    def upsert_iv_rank(self, df: pd.DataFrame) -> int:
        """
        Upsert daily IV rank snapshots.

        Stores iv_rank and iv_percentile alongside raw IV values so strategy
        agents can track whether IV is elevated or depressed over time.
        Returns rows written.
        """
        if df.empty:
            return 0
        rows = df.to_dict("records")
        sql = text("""
            INSERT INTO iv_rank_history
                (recorded_at, symbol, iv_rank, iv_percentile, current_iv,
                 iv_52w_high, iv_52w_low, provider)
            VALUES
                (:recorded_at, :symbol, :iv_rank, :iv_percentile, :current_iv,
                 :iv_52w_high, :iv_52w_low, :provider)
            ON CONFLICT (recorded_at, symbol) DO UPDATE SET
                iv_rank = EXCLUDED.iv_rank,
                iv_percentile = EXCLUDED.iv_percentile,
                current_iv = EXCLUDED.current_iv,
                iv_52w_high = EXCLUDED.iv_52w_high,
                iv_52w_low = EXCLUDED.iv_52w_low,
                provider = EXCLUDED.provider,
                fetched_at = NOW()
        """)
        with self._conn() as conn:
            conn.execute(sql, rows)
        return len(rows)

    def query_iv_rank_history(
        self,
        symbol: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Query historical IV rank series for a symbol.

        Returns daily IV rank snapshots, newest first. Strategy agents use this
        to determine if current IV is historically elevated (good for selling
        premium) or depressed (favor long premium positions).

        Args:
            symbol: Ticker symbol.
            start: Inclusive start date filter. None = all history.
            end: Inclusive end date filter. None = today.

        Returns:
            DataFrame with columns: recorded_at, symbol, iv_rank, iv_percentile,
            current_iv, iv_52w_high, iv_52w_low, provider.
        """
        sql = text("""
            SELECT recorded_at, symbol, iv_rank, iv_percentile, current_iv,
                   iv_52w_high, iv_52w_low, provider
            FROM iv_rank_history
            WHERE symbol = :symbol
              AND (:start IS NULL OR recorded_at >= :start)
              AND (:end IS NULL OR recorded_at <= :end)
            ORDER BY recorded_at ASC
        """)
        with self._conn() as conn:
            result = conn.execute(sql, {
                "symbol": symbol.upper(),
                "start": start.isoformat() if start else None,
                "end": end.isoformat() if end else None,
            })
            rows = result.fetchall()
            columns = result.keys()
        return pd.DataFrame(rows, columns=list(columns)) if rows else pd.DataFrame()

    # ── Generic Query Dispatcher ───────────────────────────────────────────

    def query(
        self,
        data_type: DataType,
        symbol: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Dispatch to the correct query method by data type.

        This is the primary entry point used by MarketDataService.get() when
        coverage is complete and data should be read from local storage.

        Args:
            data_type: Determines which table and method to use.
            symbol: Ticker symbol (normalized to uppercase internally).
            start: Optional start date for range queries.
            end: Optional end date for range queries.

        Returns:
            DataFrame with the requested data, or empty DataFrame if not found.
        """
        dispatch = {
            DataType.OHLCV:          lambda: self.query_ohlcv(symbol, start, end),
            DataType.OHLCV_INTRADAY: lambda: self.query_ohlcv(symbol, start, end, Interval.ONE_HOUR),
            DataType.FUNDAMENTALS:   lambda: self.query_fundamentals(symbol),
            DataType.EARNINGS:       lambda: self.query_earnings(symbol),
            DataType.DIVIDENDS:      lambda: self.query_dividends(symbol),
            DataType.NEWS_SENTIMENT: lambda: self.query_news_sentiment(symbol, start, end),
            DataType.OPTIONS_CHAIN:  lambda: self.query_options_snapshot(symbol, snapshot_date=end),
            DataType.IV_RANK:        lambda: self.query_iv_rank_history(symbol, start, end),
        }
        fn = dispatch.get(data_type)
        if fn is None:
            logger.warning(f"TimescaleStore.query not implemented for {data_type} — returning empty DataFrame")
            return pd.DataFrame()
        return fn()

    # ── Health Check ──────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Return True if the database connection is healthy."""
        try:
            with self._conn() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception:
            return False
