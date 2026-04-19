"""
market_data/iv_backfill.py
Historical IV rank backfill using Databento OPRA.PILLAR.

Fetches option chain data for each trading day in a date range, computes
ATM implied volatility via Black-Scholes, and stores complete IV rank rows
(iv_rank, iv_percentile, iv_52w_high, iv_52w_low) in the local database.

Designed to run once as a pre-backtest setup step for VRP/vol strategies.
Subsequent runs are safe — already-stored dates are skipped (resume-friendly).
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import TYPE_CHECKING, Optional

import pandas as pd

if TYPE_CHECKING:
    from market_data.providers.databento import DatabentoProvider
    from market_data.storage.timescale import TimescaleStore

logger = logging.getLogger(__name__)


def trading_days(start: date, end: date) -> list[date]:
    """Return all weekdays (Mon–Fri) between start and end inclusive."""
    days: list[date] = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def backfill_iv_rank(
    symbol: str,
    start_date: date,
    end_date: date,
    provider: "DatabentoProvider",
    store: "TimescaleStore",
    underlying_prices: dict[date, float],
    progress_callback=None,
) -> tuple[int, int]:
    """
    Backfill historical IV rank for a symbol into the local database.

    For each weekday in [start_date, end_date]:
      1. Skip if already stored in iv_rank_history.
      2. Look up the underlying close price for that date.
      3. Fetch OPRA.PILLAR definition + ohlcv-1d from Databento.
      4. Compute ATM IV via Black-Scholes (see compute_atm_iv_from_opra).
      5. Compute iv_rank / iv_percentile from all history stored so far.
      6. Upsert the complete row into iv_rank_history.

    Dates with no Databento data (holidays, weekends that slipped through,
    dates before listing) are silently skipped and counted as skipped.

    Args:
        symbol:             Ticker, e.g. "SPY".
        start_date:         First date to backfill.
        end_date:           Last date to backfill (inclusive).
        provider:           Initialised DatabentoProvider instance.
        store:              TimescaleStore instance (direct DB access for upserts).
        underlying_prices:  {date: close_price} for the underlying. Only dates
                            present in this dict will be processed; others are skipped.
        progress_callback:  Optional callable(current, total, snap, status) for
                            Rich progress updates from the CLI.

    Returns:
        (dates_processed, dates_skipped) counts.
    """
    from market_data.providers.databento import DATASET_OPTIONS, compute_atm_iv_from_opra

    sym = symbol.upper()
    days = trading_days(start_date, end_date)
    total = len(days)

    # Load already-stored dates so we can skip them (resume on interruption)
    existing_df = store.query_iv_rank_history(sym, start=start_date, end=end_date)
    stored_dates: set[date] = set()
    if not existing_df.empty:
        for d in existing_df["recorded_at"]:
            stored_dates.add(d if isinstance(d, date) else d.date())

    logger.info(
        "[iv_backfill] %s: %d weekdays in range, %d already stored, %d to fetch",
        sym, total, len(stored_dates), total - len(stored_dates),
    )

    client = provider._get_client()
    parent_sym = f"{sym}.OPT"

    processed = 0
    skipped = 0

    for idx, snap in enumerate(days, start=1):
        if progress_callback:
            progress_callback(idx, total, snap, "checking")

        if snap in stored_dates:
            skipped += 1
            continue

        underlying_price = underlying_prices.get(snap)
        if not underlying_price:
            logger.debug("[iv_backfill] No underlying price for %s on %s — skipping", sym, snap)
            skipped += 1
            if progress_callback:
                progress_callback(idx, total, snap, "skip:no_price")
            continue

        # Fetch OPRA chain for this date (2 API calls: definition + ohlcv-1d)
        start_str = snap.isoformat()
        end_str   = (snap + timedelta(days=1)).isoformat()

        try:
            defs_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="definition",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as exc:
            logger.debug("[iv_backfill] definition failed for %s on %s: %s", sym, snap, exc)
            skipped += 1
            if progress_callback:
                progress_callback(idx, total, snap, "skip:api_error")
            continue

        if defs_df.empty:
            # Holiday or pre-listing — no data for this date
            skipped += 1
            if progress_callback:
                progress_callback(idx, total, snap, "skip:no_data")
            continue

        defs_df = defs_df[defs_df["instrument_class"].isin(["C", "P"])].copy()
        defs_df = defs_df.drop_duplicates(subset="instrument_id", keep="last")

        try:
            ohlcv_df = client.timeseries.get_range(
                dataset=DATASET_OPTIONS,
                symbols=[parent_sym],
                stype_in="parent",
                schema="ohlcv-1d",
                start=start_str,
                end=end_str,
            ).to_df()
        except Exception as exc:
            logger.debug("[iv_backfill] ohlcv-1d failed for %s on %s: %s", sym, snap, exc)
            ohlcv_df = pd.DataFrame()

        atm_iv = compute_atm_iv_from_opra(defs_df, ohlcv_df, underlying_price, snap)
        if atm_iv is None:
            logger.debug("[iv_backfill] Could not compute ATM IV for %s on %s", sym, snap)
            skipped += 1
            if progress_callback:
                progress_callback(idx, total, snap, "skip:no_iv")
            continue

        # Compute rank / percentile from ALL history accumulated so far in the DB
        hist_df = store.query_iv_rank_history(sym, end=snap - timedelta(days=1))
        hist_ivs: list[float] = (
            hist_df["current_iv"].dropna().tolist() if not hist_df.empty else []
        )

        all_ivs = hist_ivs + [atm_iv]
        iv_52w_high = max(all_ivs)
        iv_52w_low  = min(all_ivs)
        iv_range    = iv_52w_high - iv_52w_low

        iv_rank = (
            (atm_iv - iv_52w_low) / iv_range * 100
            if iv_range > 0 else 50.0
        )
        iv_percentile = (
            sum(1 for iv in hist_ivs if iv < atm_iv) / len(hist_ivs) * 100
            if hist_ivs else 50.0
        )

        upsert_df = pd.DataFrame([{
            "recorded_at":   snap,
            "symbol":        sym,
            "iv_rank":       round(iv_rank, 2),
            "iv_percentile": round(iv_percentile, 2),
            "current_iv":    round(atm_iv, 6),
            "iv_52w_high":   round(iv_52w_high, 6),
            "iv_52w_low":    round(iv_52w_low, 6),
            "provider":      "databento",
        }])
        store.upsert_iv_rank(upsert_df)
        processed += 1

        if progress_callback:
            progress_callback(idx, total, snap, f"ok:iv={atm_iv:.4f} rank={iv_rank:.1f}")

        if processed % 20 == 0:
            logger.info(
                "[iv_backfill] %s: %d/%d processed, %d skipped",
                sym, processed, total, skipped,
            )

    logger.info(
        "[iv_backfill] %s complete: %d processed, %d skipped",
        sym, processed, skipped,
    )
    return processed, skipped
