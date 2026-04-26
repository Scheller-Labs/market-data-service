"""
market_data/service.py
Core Market Data Service orchestration.
This is where coverage check → gap analysis → delta fetch → store → return all happens.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Optional, Any

try:
    from minio import Minio
except ImportError:
    Minio = None  # type: ignore[assignment,misc]

import pandas as pd

from market_data.config import settings
from market_data.models import (
    DataType, Interval, CoverageStatus,
    DataResponse, StatusResponse, BatchResponse, HealthResponse
)
from market_data.cache.coverage import CoverageManifest
from market_data.cache.redis_cache import RedisCache
from market_data.storage.timescale import TimescaleStore
from market_data.providers.router import ProviderRouter

logger = logging.getLogger(__name__)


class MarketDataService:
    """
    Main orchestration layer.
    Called by the CLI and directly by agent code via the client wrapper.

    The core get() flow:
      1. Check Redis hot cache (sub-ms)
      2. Check coverage manifest (SQLite, ~1ms)
      3. If complete → query TimescaleDB
      4. If gaps → fetch only missing ranges from provider
      5. Write new data → TimescaleDB + Redis + update coverage manifest
      6. Return unified DataResponse
    """

    def __init__(self):
        self.coverage = CoverageManifest()
        self.redis = RedisCache()
        self.store = TimescaleStore()
        self.router = ProviderRouter()

    # ── Primary: get() ────────────────────────────────────────────────────

    def get(
        self,
        symbol: str,
        data_type: DataType,
        start: date,
        end: date,
        interval: Interval = Interval.ONE_DAY,
        force_refresh: bool = False,
        preferred_provider: Optional[str] = None,
        output_format: str = "json",
    ) -> DataResponse:
        """
        Cache-first, delta-fetch data retrieval.
        Returns a DataResponse with unified schema regardless of source.
        """
        symbol = symbol.upper()
        logger.info(f"get({symbol}, {data_type}, {start}→{end})")

        # ── 1. Force refresh: invalidate caches ──────────────────────────
        if force_refresh:
            logger.info(f"Force refresh: invalidating cache for {symbol} {data_type}")
            self.redis.invalidate(symbol, data_type, interval)
            self.coverage.invalidate(symbol, data_type, interval)

        # ── 2. Check coverage manifest ────────────────────────────────────
        coverage_report = self.coverage.check(symbol, data_type, start, end, interval)

        if coverage_report.status == CoverageStatus.COMPLETE and not force_refresh:
            # ── 3a. Full cache hit — query TimescaleDB directly ───────────
            logger.info(f"Coverage complete for {symbol} {data_type} — serving from local store")
            df = self.store.query(data_type, symbol, start, end)
            return self._build_response(df, symbol, data_type, interval, "timescaledb",
                                        CoverageStatus.COMPLETE, [])

        # ── 4. Fetch missing gaps from provider ───────────────────────────
        all_dfs = []
        fetch_sources = set()

        for gap in coverage_report.gaps:
            logger.info(f"Fetching gap: {symbol} {data_type} {gap.start}→{gap.end}")
            if settings.dry_run:
                logger.info(f"[DRY RUN] Would fetch {symbol} {data_type} {gap.start}→{gap.end}")
                continue

            try:
                provider = self.router.select(data_type, preferred=preferred_provider)
                df_gap = self._fetch_from_provider(
                    provider, symbol, data_type, gap.start, gap.end, interval
                )
                if not df_gap.empty:
                    all_dfs.append(df_gap)
                    fetch_sources.add(f"api:{provider.name}")
                    # ── 5. Write new data to store ──────────────────────
                    self._persist(df_gap, data_type, interval)
                    self.coverage.record(
                        symbol, data_type, gap.start, gap.end,
                        provider.name, interval, len(df_gap)
                    )
                else:
                    logger.warning(f"No data returned for {symbol} {data_type} {gap.start}→{gap.end}")
            except Exception as e:
                logger.error(f"Failed to fetch gap {gap}: {e}", exc_info=True)

        # ── 6. Query full requested range from local store ────────────────
        final_df = self.store.query(data_type, symbol, start, end)
        source = "merged" if len(fetch_sources) > 0 else "timescaledb"

        # Recheck coverage after fetches
        final_coverage = self.coverage.check(symbol, data_type, start, end, interval)

        return self._build_response(
            final_df, symbol, data_type, interval,
            source, final_coverage.status, final_coverage.gaps
        )

    # ── IV Rank ────────────────────────────────────────────────────────────

    def get_iv_rank(
        self,
        symbol: str,
        lookback_days: int = 252,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Return the IV rank history for a symbol, ingesting today's data if missing.

        Flow:
          1. If today's row is absent (or force_refresh): call provider.fetch_iv_rank()
             to get current ATM IV.
          2. Query lookback_days of stored history.
          3. Compute iv_rank, iv_percentile, iv_52w_high, iv_52w_low from that history.
          4. Upsert today's complete row.
          5. Return the full updated history DataFrame.

        iv_rank     = (current_iv - iv_52w_low) / (iv_52w_high - iv_52w_low) * 100
        iv_percentile = % of history days where stored IV was below current_iv

        When fewer than 30 days of history exist the rank is computed from whatever
        data is available; callers should check the 'rows' count.
        """
        symbol = symbol.upper()
        today = date.today()

        today_df = self.store.query_iv_rank_history(symbol, start=today, end=today)

        if today_df.empty or force_refresh:
            raw_df = pd.DataFrame()
            for provider in self.router.iter_for_type(DataType.IV_RANK):
                try:
                    raw_df = provider.fetch_iv_rank(symbol)
                    if not raw_df.empty:
                        break
                    logger.warning("IV rank provider %s returned empty for %s", provider.name, symbol)
                except Exception as exc:
                    logger.warning("Could not fetch IV rank for %s via %s: %s", symbol, provider.name, exc)

            # Fallback 1: compute ATM IV from OPRA via Databento using stored spot price.
            # Handles NYSE/Arca-listed underlyings (SPY, QQQ) where XNAS.ITCH is unavailable.
            if raw_df.empty:
                try:
                    from market_data.models import Interval
                    spot_df = self.store.query_ohlcv(
                        symbol,
                        start=today - timedelta(days=10),
                        end=today,
                        interval=Interval.ONE_DAY,
                    )
                    if not spot_df.empty:
                        spot_price = float(spot_df.iloc[-1]["close"])
                        databento_prov = self.router.get("databento")
                        if databento_prov is not None:
                            atm_iv = databento_prov.fetch_atm_iv_with_spot(symbol, spot_price)
                            if atm_iv is not None:
                                raw_df = pd.DataFrame([{
                                    "recorded_at": today,
                                    "symbol":      symbol,
                                    "current_iv":  round(atm_iv, 6),
                                    "provider":    "databento",
                                }])
                                logger.info(
                                    "Computed ATM IV for %s from OPRA+stored spot (%.2f): %.4f",
                                    symbol, spot_price, atm_iv,
                                )
                except Exception as exc:
                    logger.warning("OPRA+stored-spot IV fallback failed for %s: %s", symbol, exc)

            # Fallback 2: compute ATM IV from stored options_snapshots (last resort).
            if raw_df.empty:
                chain_df = self.store.query_options_snapshot(symbol)
                if not chain_df.empty:
                    current_iv = self._atm_iv_from_stored_chain(chain_df)
                    if current_iv is not None:
                        prov = (str(chain_df["provider"].iloc[0])
                                if "provider" in chain_df.columns else "stored")
                        raw_df = pd.DataFrame([{
                            "recorded_at": date.today(),
                            "symbol":      symbol,
                            "current_iv":  current_iv,
                            "provider":    prov,
                        }])
                        logger.info(
                            "Computed ATM IV for %s from stored options chain: %.4f",
                            symbol, current_iv,
                        )

            if not raw_df.empty:
                current_iv = float(raw_df.iloc[0]["current_iv"])
                prov_name = str(raw_df.iloc[0]["provider"])

                # Pull history (excluding today so we don't double-count)
                hist_start = today - timedelta(days=lookback_days)
                hist_df = self.store.query_iv_rank_history(
                    symbol, start=hist_start, end=today - timedelta(days=1)
                )
                hist_ivs: list[float] = (
                    hist_df["current_iv"].dropna().tolist() if not hist_df.empty else []
                )

                all_ivs = hist_ivs + [current_iv]
                iv_52w_high = max(all_ivs)
                iv_52w_low  = min(all_ivs)
                iv_range    = iv_52w_high - iv_52w_low

                iv_rank = (
                    (current_iv - iv_52w_low) / iv_range * 100
                    if iv_range > 0 else 50.0
                )
                iv_percentile = (
                    sum(1 for iv in hist_ivs if iv < current_iv) / len(hist_ivs) * 100
                    if hist_ivs else 50.0
                )

                upsert_df = pd.DataFrame([{
                    "recorded_at":   today,
                    "symbol":        symbol,
                    "iv_rank":       round(iv_rank, 2),
                    "iv_percentile": round(iv_percentile, 2),
                    "current_iv":    current_iv,
                    "iv_52w_high":   iv_52w_high,
                    "iv_52w_low":    iv_52w_low,
                    "provider":      prov_name,
                }])
                self.store.upsert_iv_rank(upsert_df)
                logger.info(
                    "Ingested IV rank for %s: rank=%.1f, percentile=%.1f, iv=%.4f",
                    symbol, iv_rank, iv_percentile, current_iv,
                )

        # Return full history (includes the row we just upserted)
        hist_start = today - timedelta(days=lookback_days)
        return self.store.query_iv_rank_history(symbol, start=hist_start, end=today)

    # ── Status ────────────────────────────────────────────────────────────

    def status(
        self,
        symbol: str,
        data_type: DataType,
        start: date,
        end: date,
        interval: Optional[Interval] = None,
    ) -> StatusResponse:
        """Report coverage without fetching anything."""
        symbol = symbol.upper()
        report = self.coverage.check(symbol, data_type, start, end, interval)
        covered = self.coverage.get_covered_ranges(symbol, data_type, interval)

        return StatusResponse(
            symbol=symbol,
            data_type=data_type,
            coverage=report.status,
            available_ranges=[
                {"start": str(s), "end": str(e)} for s, e in covered
            ],
            gaps=report.gaps,
        )

    # ── Batch ─────────────────────────────────────────────────────────────

    def batch(
        self,
        symbols: list[str],
        data_type: DataType,
        start: date,
        end: date,
        interval: Interval = Interval.ONE_DAY,
        max_workers: Optional[int] = None,
    ) -> BatchResponse:
        """Parallel fetch for multiple symbols."""
        workers = max_workers or settings.max_batch_workers
        succeeded, failed = [], []
        results = {}

        def _fetch_one(sym: str):
            return sym, self.get(sym, data_type, start, end, interval)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {executor.submit(_fetch_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    _, response = future.result()
                    results[sym] = response
                    succeeded.append(sym)
                except Exception as e:
                    logger.error(f"Batch fetch failed for {sym}: {e}")
                    failed.append(sym)

        return BatchResponse(
            requested=symbols,
            succeeded=succeeded,
            failed=failed,
            results=results,
        )

    # ── Warm Cache ────────────────────────────────────────────────────────

    def warm(
        self,
        symbols: list[str],
        data_types: list[DataType],
        days: int = 365,
    ) -> dict[str, Any]:
        """Pre-populate local store for a watchlist."""
        end = date.today()
        start = end - timedelta(days=days)
        report = {"symbols": symbols, "data_types": [dt.value for dt in data_types], "results": {}}

        for dt in data_types:
            result = self.batch(symbols, dt, start, end)
            report["results"][dt.value] = {
                "succeeded": result.succeeded,
                "failed": result.failed,
            }
            logger.info(f"Warmed {dt}: {len(result.succeeded)} ok, {len(result.failed)} failed")

        return report

    # ── Health ────────────────────────────────────────────────────────────

    def health(self) -> HealthResponse:
        """Check connectivity of all infrastructure components."""
        ts_ok = self.store.ping()
        redis_ok = self.redis.ping()

        try:
            if Minio is None:
                raise RuntimeError("minio package not installed")
            mc = Minio(
                settings.minio_endpoint,
                access_key=settings.minio_access_key,
                secret_key=settings.minio_secret_key,
                secure=settings.minio_secure,
            )
            mc.list_buckets()
            minio_ok = True
        except Exception:
            minio_ok = False

        provider_health = self.router.health_check_all()
        overall = ts_ok and redis_ok and any(provider_health.values())

        return HealthResponse(
            timescaledb=ts_ok,
            redis=redis_ok,
            minio=minio_ok,
            providers=provider_health,
            overall=overall,
        )

    # ── Internal Helpers ──────────────────────────────────────────────────

    @staticmethod
    def _atm_iv_from_stored_chain(chain_df: pd.DataFrame) -> Optional[float]:
        """
        Compute ATM IV from a stored options chain DataFrame.

        Algorithm:
          1. Filter to nearest expiry with 7–60 DTE (from today).
          2. Use delta ≈ ±0.50 to identify the ATM call and ATM put.
          3. Average their implied_volatility values.
          4. Fall back to median IV of all near-term options if delta is missing.

        Returns None if no IV data is available.
        """
        today = date.today()
        df = chain_df.dropna(subset=["implied_volatility"]).copy()
        df = df[df["implied_volatility"] > 0]
        if df.empty:
            return None

        # Compute DTE per row
        def _dte(exp) -> int:
            try:
                d = exp if isinstance(exp, date) else date.fromisoformat(str(exp)[:10])
                return (d - today).days
            except Exception:
                return -1

        if "expiration_date" in df.columns:
            df["_dte"] = df["expiration_date"].apply(_dte)
            near = df[(df["_dte"] >= 7) & (df["_dte"] <= 60)]
            if near.empty:
                near = df[df["_dte"] >= 0]  # any unexpired
            if not near.empty:
                min_dte = near["_dte"].min()
                df = near[near["_dte"] == min_dte]

        # Delta-based ATM selection
        if "delta" in df.columns and "option_type" in df.columns:
            atm_ivs: list[float] = []
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

        # Fallback: median IV of near-term options
        return float(df["implied_volatility"].median())

    def _fetch_from_provider(
        self, provider, symbol: str, data_type: DataType,
        start: date, end: date, interval: Interval
    ) -> pd.DataFrame:
        """Dispatch to the correct provider method by data type."""
        if data_type in (DataType.OHLCV, DataType.OHLCV_INTRADAY, DataType.FUTURES_OHLCV):
            return provider.fetch_ohlcv(symbol, start, end, interval)
        elif data_type == DataType.FUNDAMENTALS:
            return provider.fetch_fundamentals(symbol)
        elif data_type == DataType.OPTIONS_CHAIN:
            # Pass the start date as the snapshot date so providers fetch
            # the correct historical day rather than defaulting to yesterday.
            return provider.fetch_options_chain(symbol, snapshot_date=start)
        elif data_type == DataType.NEWS_SENTIMENT:
            return provider.fetch_news_sentiment(symbol, start, end)
        elif data_type == DataType.EARNINGS:
            return provider.fetch_earnings(symbol)
        elif data_type == DataType.DIVIDENDS:
            return provider.fetch_dividends(symbol)
        else:
            raise NotImplementedError(f"No fetch dispatch for {data_type}")

    def _persist(self, df: pd.DataFrame, data_type: DataType, interval: Interval):
        """Write a DataFrame to TimescaleDB based on data type."""
        if data_type in (DataType.OHLCV, DataType.OHLCV_INTRADAY, DataType.FUTURES_OHLCV):
            self.store.upsert_ohlcv(df)
        elif data_type == DataType.FUNDAMENTALS:
            self.store.upsert_fundamentals(df)
        elif data_type == DataType.EARNINGS:
            self.store.upsert_earnings(df)
        elif data_type == DataType.DIVIDENDS:
            self.store.upsert_dividends(df)
        elif data_type == DataType.NEWS_SENTIMENT:
            self.store.upsert_news_sentiment(df)
        elif data_type == DataType.OPTIONS_CHAIN:
            self.store.upsert_options_snapshot(df)
        else:
            logger.warning(f"No persist handler for {data_type} — data not stored")

    def _build_response(
        self,
        df: pd.DataFrame,
        symbol: str,
        data_type: DataType,
        interval: Interval,
        source: str,
        coverage_status: CoverageStatus,
        gaps: list,
    ) -> DataResponse:
        rows_data = df.to_dict("records") if not df.empty else []
        schema = list(df.columns) if not df.empty else []

        # Serialize non-JSON-serializable types
        for row in rows_data:
            for k, v in row.items():
                if isinstance(v, (datetime, date)):
                    row[k] = v.isoformat()
                elif hasattr(v, "item"):  # numpy scalar
                    row[k] = v.item()

        return DataResponse(
            symbol=symbol,
            data_type=data_type,
            interval=interval.value,
            source=source,
            coverage=coverage_status,
            gaps=gaps,
            rows=len(rows_data),
            fetched_at=datetime.now(tz=timezone.utc).isoformat(),
            schema=schema,
            data=rows_data,
        )
