#!/usr/bin/env python3
"""
Backfill iv_rank_history from stored options_snapshots.

For each date in options_snapshots, computes ATM IV from near-expiry contracts
and writes it to iv_rank_history. Then recomputes iv_rank and iv_percentile
for each date using the full rolling history.

Must be run once after the initial import_databento_options.py run.

Usage:
    cd /home/bobsc/Projects/agent-trading-firm/market-data-service
    .venv/bin/python scripts/backfill_iv_rank.py
"""

import logging
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import sqlalchemy as sa

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from market_data.storage.timescale import TimescaleStore

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

IV_MIN_DTE = 7
IV_MAX_DTE = 60


def atm_iv_for_date(store: TimescaleStore, symbol: str, snap_date: date) -> float | None:
    """Compute ATM IV from stored options chain for a given date."""
    df = store.query_options_snapshot(symbol, snapshot_date=snap_date)
    if df.empty:
        return None

    df = df.dropna(subset=["implied_volatility"])
    df = df[df["implied_volatility"] > 0]
    if df.empty:
        return None

    # Filter to target DTE window
    def dte(exp):
        try:
            d = exp if isinstance(exp, date) else date.fromisoformat(str(exp)[:10])
            return (d - snap_date).days
        except Exception:
            return -1

    if "expiration_date" in df.columns:
        df = df.copy()
        df["_dte"] = df["expiration_date"].apply(dte)
        near = df[(df["_dte"] >= IV_MIN_DTE) & (df["_dte"] <= IV_MAX_DTE)]
        if near.empty:
            near = df[df["_dte"] >= 0]
        if not near.empty:
            min_dte = near["_dte"].min()
            df = near[near["_dte"] == min_dte]

    # ATM via delta if available
    if "delta" in df.columns and "option_type" in df.columns:
        atm_ivs = []
        calls = df[df["option_type"].str.lower() == "call"].dropna(subset=["delta"])
        puts  = df[df["option_type"].str.lower() == "put"].dropna(subset=["delta"])
        if not calls.empty:
            calls = calls.copy()
            calls["_dd"] = (calls["delta"] - 0.5).abs()
            atm_ivs.append(float(calls.nsmallest(1, "_dd")["implied_volatility"].iloc[0]))
        if not puts.empty:
            puts = puts.copy()
            puts["_dd"] = (puts["delta"] + 0.5).abs()
            atm_ivs.append(float(puts.nsmallest(1, "_dd")["implied_volatility"].iloc[0]))
        if atm_ivs:
            return sum(atm_ivs) / len(atm_ivs)

    return float(df["implied_volatility"].median())


def main() -> None:
    store = TimescaleStore()

    # ── Get all distinct snapshot dates ──────────────────────────────────────
    with store._conn() as conn:
        rows = conn.execute(sa.text(
            "SELECT DISTINCT snapshot_at::DATE FROM options_snapshots "
            "WHERE symbol = 'SPY' ORDER BY 1"
        )).fetchall()
    snap_dates = [r[0] for r in rows]
    log.info(f"Found {len(snap_dates)} snapshot dates ({snap_dates[0]} → {snap_dates[-1]})")

    # ── Compute ATM IV for each date ──────────────────────────────────────────
    iv_by_date: dict[date, float] = {}
    for i, d in enumerate(snap_dates):
        iv = atm_iv_for_date(store, "SPY", d)
        if iv is not None:
            iv_by_date[d] = iv
        if (i + 1) % 50 == 0:
            log.info(f"  Computed ATM IV for {i+1}/{len(snap_dates)} dates "
                     f"({len(iv_by_date)} with valid IV)")

    log.info(f"ATM IV computed for {len(iv_by_date)}/{len(snap_dates)} dates")

    # ── Compute rolling iv_rank and iv_percentile ─────────────────────────────
    sorted_dates = sorted(iv_by_date)
    rows_to_write = []
    for i, d in enumerate(sorted_dates):
        current_iv = iv_by_date[d]
        hist_ivs = [iv_by_date[h] for h in sorted_dates[:i]]  # all prior dates

        if hist_ivs:
            iv_52w_high = max(hist_ivs + [current_iv])
            iv_52w_low  = min(hist_ivs + [current_iv])
            iv_range    = iv_52w_high - iv_52w_low
            iv_rank     = (current_iv - iv_52w_low) / iv_range * 100 if iv_range > 0 else 50.0
            iv_percentile = sum(1 for h in hist_ivs if h < current_iv) / len(hist_ivs) * 100
        else:
            iv_52w_high = iv_52w_low = current_iv
            iv_rank = iv_percentile = 50.0

        rows_to_write.append({
            "recorded_at":   d,
            "symbol":        "SPY",
            "iv_rank":       round(iv_rank, 2),
            "iv_percentile": round(iv_percentile, 2),
            "current_iv":    current_iv,
            "iv_52w_high":   iv_52w_high,
            "iv_52w_low":    iv_52w_low,
            "provider":      "databento",
        })

    df = pd.DataFrame(rows_to_write)
    log.info(f"Writing {len(df)} rows to iv_rank_history ...")
    store.upsert_iv_rank(df)
    log.info("Backfill complete.")
    log.info(f"IV rank range: {df['iv_rank'].min():.1f} – {df['iv_rank'].max():.1f}")
    log.info(f"Current IV range: {df['current_iv'].min():.4f} – {df['current_iv'].max():.4f}")


if __name__ == "__main__":
    main()
