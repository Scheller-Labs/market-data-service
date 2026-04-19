#!/usr/bin/env python3
"""
Import Databento SPY options data from 2to10_options DuckDB into MDS TimescaleDB.

Source:  /home/bobsc/Projects/2to10_options/data/backtest.duckdb
         Table: historical_options  (14.9M rows, 2024-03-08 → 2026-02-25)
         Table: historical_equities (843 rows SPY OHLCV)
         Schema: ts_event, rtype, publisher_id, instrument_id,
                 open, high, low, close, volume, symbol (OCC 21-char)

Target:  MDS TimescaleDB → options_snapshots

OCC symbol format (21 chars, 1-indexed):
    chars  1–6   root symbol, space-padded  e.g. 'SPY   '
    chars  7–12  expiration YYMMDD          e.g. '240311'
    char   13    option type C or P         e.g. 'P'
    chars 14–21  strike * 1000, zero-padded e.g. '00482000' = $482.00

Deduplication:  OPRA has multiple exchange publishers (publisher_id) for the same
contract on the same day.  We aggregate with AVG(close) and SUM(volume)
per (symbol, date) before loading.

Mapping:
    ts_event (DATE)   → snapshot_at (midnight UTC)
    root symbol       → symbol
    expiration YYMMDD → expiration_date
    C/P               → option_type (call/put)
    strike / 1000     → strike
    AVG(close)        → last
    SUM(volume)       → volume
    BS IV (computed)  → implied_volatility (near-ATM only, DTE 1–90)
    SPY close         → underlying_price
    open/high/low/bid/ask/OI/Greeks → NULL (not in Databento OHLCV-1D)
    provider          → 'databento'

IV Computation:
    Black-Scholes inversion (Brent's method) using:
      - option last price (AVG close across exchanges)
      - SPY close on snapshot date as underlying price
      - risk-free rate: 5.0% (conservative constant)
      - Only computed for: DTE 1–90, strike within 15% of SPY close, last > 0

Requirements:
    pip install duckdb scipy   (not in pyproject.toml by default — one-time tools)
    MDS_TIMESCALE_URL env var must point to a running TimescaleDB instance.

Usage:
    cd /home/bobsc/Projects/agent-trading-firm/market-data-service
    MDS_TIMESCALE_URL=postgresql://user:pass@localhost:5432/marketdata \\
        .venv/bin/python scripts/import_databento_options.py

    # Dry-run (parse + print stats, no DB writes):
    .venv/bin/python scripts/import_databento_options.py --dry-run

    # Resume from a specific date (skip already-imported days):
    .venv/bin/python scripts/import_databento_options.py --start-date 2025-01-01

    # Skip IV computation (faster, all IV will be NULL):
    .venv/bin/python scripts/import_databento_options.py --no-iv
"""

import argparse
import logging
import sys
from datetime import date, datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.optimize import brentq
from scipy.stats import norm

# ── resolve project root so we can import market_data without installing ───────
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

try:
    import duckdb
except ImportError:
    print("ERROR: duckdb not installed.  Run:  .venv/bin/pip install duckdb")
    sys.exit(1)

from market_data.storage.timescale import TimescaleStore

# ── constants ──────────────────────────────────────────────────────────────────

DUCKDB_PATH = "/home/bobsc/Projects/2to10_options/data/backtest.duckdb"
PROVIDER = "databento"
BATCH_DATES = 10          # process this many calendar dates per TimescaleDB batch
LOG_EVERY_N_DATES = 20    # print progress every N dates

RISK_FREE_RATE  = 0.05    # constant 5% — conservative approximation
IV_MAX_MONEYNESS = 0.15   # only compute IV for strikes within 15% of underlying
IV_MIN_DTE       = 1      # skip same-day expiry (can't compute meaningful IV)
IV_MAX_DTE       = 90     # only near-term options

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Black-Scholes IV ───────────────────────────────────────────────────────────

def _bs_price(S: float, K: float, T: float, r: float, sigma: float, is_call: bool) -> float:
    """Black-Scholes option price (no dividends)."""
    sqrt_T = np.sqrt(T)
    d1 = (np.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * sqrt_T)
    d2 = d1 - sigma * sqrt_T
    if is_call:
        return float(S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2))
    return float(K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1))


def compute_iv(price: float, S: float, K: float, T: float, r: float, is_call: bool) -> float | None:
    """
    Implied volatility via Brent's method.

    Returns None when:
    - price ≤ 0, S ≤ 0, K ≤ 0, or T ≤ 0
    - price is below intrinsic (no real solution exists)
    - solver fails to converge in bracket [0.001, 20.0]
    """
    if price <= 0 or S <= 0 or K <= 0 or T <= 0:
        return None
    try:
        def obj(sigma: float) -> float:
            return _bs_price(S, K, T, r, sigma, is_call) - price
        lo, hi = 0.001, 20.0
        if obj(lo) * obj(hi) >= 0:
            return None
        return float(brentq(obj, lo, hi, xtol=1e-6, maxiter=100))
    except (ValueError, RuntimeError):
        return None


def _add_iv_columns(df: pd.DataFrame, spy_close: dict[date, float]) -> pd.DataFrame:
    """
    Compute IV for eligible rows, adding underlying_price and implied_volatility columns.

    Eligibility: DTE in [IV_MIN_DTE, IV_MAX_DTE], |K-S|/S ≤ IV_MAX_MONEYNESS, last > 0.

    Uses a vectorised pre-filter to select eligible rows, then calls brentq only for
    those — typically ~40-50% of contracts, keeping runtime manageable.
    """
    df = df.copy()

    # ── underlying_price ──────────────────────────────────────────────────────
    df["underlying_price"] = df["snapshot_date"].map(spy_close)

    # ── pre-filter: find rows worth computing IV for ──────────────────────────
    S_arr   = pd.to_numeric(df["underlying_price"], errors="coerce").to_numpy()
    K_arr   = df["strike"].to_numpy(dtype=float)
    price_arr = df["last"].to_numpy(dtype=float)

    snap_dates = df["snapshot_date"].to_numpy()
    exp_dates  = df["expiration_date"].to_numpy()
    dte_arr    = np.array([(e - s).days if hasattr(e, "days") or hasattr((e-s), "days")
                           else 0
                           for e, s in zip(exp_dates, snap_dates)], dtype=float)

    valid = (
        (S_arr > 0) &
        ~np.isnan(S_arr) &
        (price_arr > 0) &
        ~np.isnan(price_arr) &
        (dte_arr >= IV_MIN_DTE) &
        (dte_arr <= IV_MAX_DTE) &
        (np.abs(K_arr - S_arr) / S_arr <= IV_MAX_MONEYNESS)
    )

    iv_arr = np.full(len(df), np.nan, dtype=object)
    eligible_idx = np.where(valid)[0]

    if len(eligible_idx) > 0:
        is_call_arr = (df["option_type"].to_numpy() == "call")
        for i in eligible_idx:
            result = compute_iv(
                float(price_arr[i]),
                float(S_arr[i]),
                float(K_arr[i]),
                float(dte_arr[i]) / 365.0,
                RISK_FREE_RATE,
                bool(is_call_arr[i]),
            )
            iv_arr[i] = result  # None if unconverged, float otherwise

    # Convert back: None stays None for SQL NULL
    df["implied_volatility"] = pd.array(iv_arr, dtype=object)
    return df


# ── Load SPY close prices ───────────────────────────────────────────────────────

def load_spy_closes(con: "duckdb.DuckDBPyConnection") -> dict[date, float]:
    """Return {date: close_price} from historical_equities (SPY OHLCV).

    Schema: date (TIMESTAMP_S), open, high, low, close (DOUBLE), volume, symbol.
    Prices are already in regular float dollars (not Databento fixed-point).
    """
    try:
        rows = con.execute(
            "SELECT date::DATE, close FROM historical_equities "
            "WHERE symbol = 'SPY' ORDER BY 1"
        ).fetchall()
        return {r[0]: float(r[1]) for r in rows if r[1] is not None}
    except Exception as exc:
        log.warning(f"Could not load historical_equities (IV will be NULL): {exc}")
        return {}


# ── DuckDB query ───────────────────────────────────────────────────────────────

TRANSFORM_SQL = """
SELECT
    ts_event::DATE                                  AS snapshot_date,
    TRIM(SUBSTR(symbol, 1, 6))                      AS root_symbol,
    strptime(SUBSTR(symbol, 7, 6), '%y%m%d')::DATE  AS expiration_date,
    CASE SUBSTR(symbol, 13, 1) WHEN 'C' THEN 'call' ELSE 'put' END AS option_type,
    CAST(SUBSTR(symbol, 14, 8) AS INTEGER) / 1000.0 AS strike,
    AVG(close)                                      AS last,
    SUM(volume)                                     AS volume
FROM historical_options
WHERE ts_event::DATE >= ? AND ts_event::DATE <= ?
GROUP BY 1, 2, 3, 4, 5
ORDER BY 1, 2, 3, 4
"""


def fetch_dates(con: "duckdb.DuckDBPyConnection", start_date: date, end_date: date) -> list[date]:
    """Return sorted list of distinct trading dates in the DuckDB within the range."""
    rows = con.execute(
        "SELECT DISTINCT ts_event::DATE FROM historical_options "
        "WHERE ts_event::DATE >= ? AND ts_event::DATE <= ? "
        "ORDER BY 1",
        [start_date, end_date],
    ).fetchall()
    return [r[0] for r in rows]


def to_snapshot_df(
    duck_df: pd.DataFrame,
    spy_close: dict[date, float],
    compute_iv: bool = True,
) -> pd.DataFrame:
    """
    Convert a DuckDB result chunk into the DataFrame shape expected by
    TimescaleStore.upsert_options_snapshot().

    When compute_iv=True and spy_close is populated, fills implied_volatility
    and underlying_price for near-ATM 1–90 DTE rows using Black-Scholes.
    snapshot_at is set to midnight UTC of the trading date.
    """
    df = duck_df.copy()

    # snapshot_at: midnight UTC of the trading date
    df["snapshot_at"] = pd.to_datetime(df["snapshot_date"]).dt.tz_localize("UTC")

    # expiration_date: must be Python date objects for SQLAlchemy DATE binding
    df["expiration_date"] = pd.to_datetime(df["expiration_date"]).dt.date

    # snapshot_date: must be Python date objects for IV lookup
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    df.rename(columns={"root_symbol": "symbol"}, inplace=True)
    df["provider"] = PROVIDER

    # Ensure correct dtypes — use object (Python int) so SQLAlchemy can bind NULLs
    df["volume"] = df["volume"].apply(lambda v: int(v) if pd.notna(v) else None)
    df["last"] = df["last"].astype(float)

    # ── IV + underlying_price ────────────────────────────────────────────────
    if compute_iv and spy_close:
        df = _add_iv_columns(df, spy_close)
    else:
        df["implied_volatility"] = None
        df["underlying_price"] = None

    # Columns never available from OHLCV-1D
    for col in ("bid", "ask", "open_interest", "delta", "gamma", "theta", "vega", "rho",
                "iv_rank", "iv_percentile"):
        df[col] = None

    return df[[
        "snapshot_at", "symbol", "expiration_date", "strike", "option_type",
        "bid", "ask", "last", "volume", "open_interest", "implied_volatility",
        "delta", "gamma", "theta", "vega", "rho", "iv_rank", "iv_percentile",
        "underlying_price", "provider",
    ]]


# ── main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Import Databento options data into MDS TimescaleDB")
    parser.add_argument("--start-date", default=None,
                        help="Only import from this date onward (YYYY-MM-DD). Defaults to earliest in DB.")
    parser.add_argument("--end-date", default=None,
                        help="Only import up to this date (YYYY-MM-DD). Defaults to latest in DB.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and print stats without writing to TimescaleDB.")
    parser.add_argument("--no-iv", action="store_true",
                        help="Skip Black-Scholes IV computation. Faster but implied_volatility stays NULL.")
    args = parser.parse_args()

    log.info(f"Opening DuckDB (read-only): {DUCKDB_PATH}")
    con = duckdb.connect(DUCKDB_PATH, read_only=True)

    # Determine actual date bounds in the source
    min_d, max_d = con.execute(
        "SELECT MIN(ts_event::DATE), MAX(ts_event::DATE) FROM historical_options"
    ).fetchone()
    total_rows = con.execute("SELECT COUNT(*) FROM historical_options").fetchone()[0]
    log.info(f"Source: {total_rows:,} rows  |  {min_d} → {max_d}")

    start_date = date.fromisoformat(args.start_date) if args.start_date else min_d
    end_date   = date.fromisoformat(args.end_date)   if args.end_date   else max_d

    trading_dates = fetch_dates(con, start_date, end_date)
    log.info(f"Trading dates to import: {len(trading_dates)}  ({start_date} → {end_date})")

    # ── Load SPY close prices for IV computation ─────────────────────────────
    compute_iv_flag = not args.no_iv
    spy_close: dict[date, float] = {}
    if compute_iv_flag:
        spy_close = load_spy_closes(con)
        if spy_close:
            log.info(f"Loaded SPY close prices for {len(spy_close)} dates "
                     f"({min(spy_close)} → {max(spy_close)}) — will compute BS IV")
        else:
            log.warning("No SPY close prices found — implied_volatility will be NULL")
            compute_iv_flag = False
    else:
        log.info("--no-iv: skipping Black-Scholes IV computation")

    if args.dry_run:
        log.info("DRY RUN — no data will be written to TimescaleDB")

    store = TimescaleStore() if not args.dry_run else None

    total_written = 0
    total_rows_processed = 0
    total_iv_computed = 0

    # Process in batches of BATCH_DATES calendar dates
    for batch_start in range(0, len(trading_dates), BATCH_DATES):
        batch = trading_dates[batch_start : batch_start + BATCH_DATES]
        d_from, d_to = batch[0], batch[-1]

        duck_df = con.execute(TRANSFORM_SQL, [d_from, d_to]).df()

        if duck_df.empty:
            continue

        total_rows_processed += len(duck_df)
        snap_df = to_snapshot_df(duck_df, spy_close, compute_iv=compute_iv_flag)
        iv_count = int(snap_df["implied_volatility"].notna().sum())
        total_iv_computed += iv_count

        if args.dry_run:
            log.info(
                f"[DRY RUN] {d_from} → {d_to}  |  {len(snap_df):,} contracts "
                f"({iv_count} with IV)  ({batch_start + len(batch)}/{len(trading_dates)} dates)"
            )
        else:
            written = store.upsert_options_snapshot(snap_df)
            total_written += written

            if (batch_start // BATCH_DATES) % (LOG_EVERY_N_DATES // BATCH_DATES or 1) == 0:
                pct = (batch_start + len(batch)) / len(trading_dates) * 100
                log.info(
                    f"Progress: {batch_start + len(batch)}/{len(trading_dates)} dates "
                    f"({pct:.0f}%)  |  rows written: {total_written:,}  |  IV computed: {total_iv_computed:,}"
                )

    con.close()

    if args.dry_run:
        log.info(
            f"DRY RUN complete. Would have imported {total_rows_processed:,} deduplicated contract-day rows "
            f"({total_iv_computed:,} with computed IV)."
        )
    else:
        log.info(
            f"Import complete. Total rows written: {total_written:,}  "
            f"|  rows with IV: {total_iv_computed:,}"
        )


if __name__ == "__main__":
    main()
