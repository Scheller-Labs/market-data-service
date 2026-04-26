"""
market_data/cli.py
Typer CLI — the agent-facing interface for the Market Data Service.

All commands write clean JSON to stdout for machine consumption.
All human-readable output (progress, errors) goes to stderr via Rich.

Agent usage example:
    market-data get --symbol AAPL --type ohlcv --days 365 --format json
    market-data get --symbol SPY --type options_chain --provider databento --start 2026-03-20 --end 2026-03-20
    market-data options-chain --symbol SPY --expiration 2026-03-20
    market-data iv-rank --symbol AAPL --days 252
    market-data max-pain --symbol SPY --expiration 2026-03-20
"""

import json
import sys
import logging
from datetime import date, timedelta
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from market_data.models import DataType, Interval

app = typer.Typer(
    name="market-data",
    help="Market Data Service CLI — cache-first, delta-fetch market data for trading agents.",
    add_completion=True,
)

# Stdout for agents (machine-readable), stderr for humans (Rich)
stdout = Console(file=sys.stdout, highlight=False)
stderr = Console(file=sys.stderr)

# Lazy import to avoid heavy startup overhead on simple health checks
_service = None


def _get_service():
    """Lazily instantiate the MarketDataService singleton."""
    global _service
    if _service is None:
        from market_data.service import MarketDataService
        _service = MarketDataService()
    return _service


def _setup_logging(verbose: bool) -> None:
    """Configure root logger level based on verbose flag."""
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        stream=sys.stderr,
    )


def _output_json(data: dict | list) -> None:
    """Write JSON to stdout — this is what agents parse."""
    print(json.dumps(data, default=str, indent=2))


def _output_csv(data: list[dict], schema: list[str]) -> None:
    """Write CSV to stdout."""
    import csv
    import io
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=schema, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(data)
    print(buf.getvalue(), end="")


def _resolve_date_range(
    start: Optional[str],
    end: Optional[str],
    days: Optional[int],
    default_days: int = 365,
) -> tuple[date, date]:
    """Resolve CLI date arguments to concrete start/end dates."""
    end_date = date.fromisoformat(end) if end else date.today()
    if start:
        start_date = date.fromisoformat(start)
    elif days:
        start_date = end_date - timedelta(days=days)
    else:
        start_date = end_date - timedelta(days=default_days)
    return start_date, end_date


# ── Commands ───────────────────────────────────────────────────────────────


@app.command()
def get(
    symbol: str = typer.Option(..., "--symbol", "-s", help="Ticker symbol, e.g. AAPL"),
    data_type: Optional[DataType] = typer.Option(None, "--type", "-t", help="Data type to fetch"),
    start: Optional[str] = typer.Option(None, "--start", help="Start date YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, "--end", help="End date YYYY-MM-DD"),
    days: Optional[int] = typer.Option(None, "--days", "-d", help="Number of days back from today"),
    interval: Optional[Interval] = typer.Option(None, "--interval", "-i", help="Bar interval (default: 1d)"),
    force_refresh: bool = typer.Option(False, "--force-refresh", "-f", help="Bypass cache and re-fetch"),
    provider: Optional[str] = typer.Option(None, "--provider", "-p", help="Force a specific provider"),
    format: str = typer.Option("json", "--format", help="Output format: json | csv"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Fetch market data. Returns from local cache/DB when possible; fetches gaps from upstream.
    Outputs JSON (default) or CSV to stdout.

    Examples:
      market-data get --symbol AAPL
      market-data get --symbol SPY --type ohlcv --start 2024-01-01 --end 2024-12-31
      market-data get --symbol TSLA --type ohlcv_intraday --interval 1h --days 90
      market-data get --symbol AAPL --type ohlcv --provider alpha_vantage
      market-data get --symbol SPY --type options_chain --provider databento --start 2026-03-20 --end 2026-03-20
    """
    _setup_logging(verbose)

    # Resolve optional enum defaults (avoids Python 3.12 Typer/Click enum validation issues)
    effective_type = data_type if data_type is not None else DataType.OHLCV
    effective_interval = interval if interval is not None else Interval.ONE_DAY

    start_date, end_date = _resolve_date_range(start, end, days)

    try:
        svc = _get_service()
        response = svc.get(
            symbol=symbol,
            data_type=effective_type,
            start=start_date,
            end=end_date,
            interval=effective_interval,
            force_refresh=force_refresh,
            preferred_provider=provider,
        )

        if format == "csv":
            _output_csv(response.data, response.schema)
        else:
            _output_json(response.model_dump())

    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        if verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        raise typer.Exit(code=1)


@app.command()
def status(
    symbol: str = typer.Option(..., "--symbol", "-s", help="Ticker symbol, e.g. AAPL"),
    data_type: Optional[DataType] = typer.Option(None, "--type", "-t", help="Data type to check"),
    start: Optional[str] = typer.Option(None, "--start", help="Start date YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, "--end", help="End date YYYY-MM-DD"),
    days: Optional[int] = typer.Option(365, "--days", "-d", help="Days back from today (when --start not set)"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Report local coverage for a symbol/type without fetching anything.

    Useful for agents to check what data is available before issuing a full get.

    Examples:
      market-data status --symbol AAPL
      market-data status --symbol SPY --start 2024-01-01 --end 2024-12-31
      market-data status --symbol TSLA --days 90
    """
    _setup_logging(verbose)

    effective_type = data_type if data_type is not None else DataType.OHLCV
    end_date = date.fromisoformat(end) if end else date.today()
    start_date = date.fromisoformat(start) if start else end_date - timedelta(days=days or 365)

    try:
        svc = _get_service()
        response = svc.status(symbol, effective_type, start_date, end_date)
        _output_json(response.model_dump())
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def batch(
    symbols: str = typer.Option(..., "--symbols", help="Comma-separated symbols: AAPL,TSLA,SPY"),
    data_type: Optional[DataType] = typer.Option(None, "--type", "-t"),
    days: int = typer.Option(365, "--days", "-d"),
    interval: Optional[Interval] = typer.Option(None, "--interval", help="Bar interval (default: 1d)"),
    workers: int = typer.Option(4, "--workers", "-w", help="Parallel fetch workers"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Batch fetch for multiple symbols in parallel.

    Example:
      market-data batch --symbols AAPL,TSLA,SPY --type ohlcv --days 90 --workers 8
    """
    _setup_logging(verbose)

    effective_type = data_type if data_type is not None else DataType.OHLCV
    effective_interval = interval if interval is not None else Interval.ONE_DAY

    symbol_list = [s.strip().upper() for s in symbols.split(",")]
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    try:
        svc = _get_service()
        response = svc.batch(symbol_list, effective_type, start_date, end_date,
                             effective_interval, max_workers=workers)
        _output_json(response.model_dump())
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def warm(
    watchlist: str = typer.Option(..., "--watchlist", "-w",
                                  help="Path to watchlist file (one symbol per line) or comma-separated symbols"),
    days: int = typer.Option(365, "--days", "-d"),
    types: str = typer.Option("ohlcv,fundamentals", "--types",
                               help="Comma-separated data types to warm"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Pre-populate local cache for a watchlist of symbols.

    Example:
      market-data warm --watchlist watchlist.txt --days 365 --types ohlcv,fundamentals
      market-data warm --watchlist AAPL,MSFT,SPY --types ohlcv
    """
    _setup_logging(verbose)

    # Load symbols from file or inline comma list
    import os
    if os.path.exists(watchlist):
        with open(watchlist) as f:
            symbols = [line.strip().upper() for line in f if line.strip() and not line.startswith("#")]
    else:
        symbols = [s.strip().upper() for s in watchlist.split(",")]

    data_types = [DataType(t.strip()) for t in types.split(",")]

    stderr.print(f"Warming {len(symbols)} symbols × {len(data_types)} data types × {days} days...")
    try:
        svc = _get_service()
        result = svc.warm(symbols, data_types, days)
        _output_json(result)
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def list_data(
    symbol: Optional[str] = typer.Option(None, "--symbol", "-s"),
    data_type: Optional[DataType] = typer.Option(None, "--type", "-t"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    List all locally available data from the coverage manifest.

    Useful for auditing what has been cached locally without making any API calls.
    """
    _setup_logging(verbose)
    try:
        svc = _get_service()
        records = svc.coverage.list_available(symbol=symbol, data_type=data_type)

        if not records:
            _output_json({"records": [], "count": 0})
            return

        _output_json({
            "records": [r.model_dump() for r in records],
            "count": len(records),
        })
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def health(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Check connectivity to all infrastructure components and providers.

    Exit code 0 = all systems healthy. Exit code 1 = degraded.
    """
    _setup_logging(verbose)
    stderr.print("Checking infrastructure health...")
    try:
        svc = _get_service()
        response = svc.health()
        _output_json(response.model_dump())
        if not response.overall:
            raise typer.Exit(code=1)
    except typer.Exit:
        raise
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


@app.command()
def audit(
    symbol: str = typer.Option(..., "--symbol", "-s"),
    data_type: Optional[DataType] = typer.Option(None, "--type", "-t"),
    days: int = typer.Option(365, "--days", "-d"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Data quality audit — check for gaps, nulls, price anomalies.

    Reports coverage, null field counts, and detects large price moves that
    may indicate bad data. Exit code 0 = audit complete (not necessarily clean).
    """
    _setup_logging(verbose)

    effective_type = data_type if data_type is not None else DataType.OHLCV
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    try:
        svc = _get_service()
        response = svc.get(symbol, effective_type, start_date, end_date)

        import pandas as pd
        df = pd.DataFrame(response.data, columns=response.schema)
        audit_result = {
            "symbol": symbol,
            "data_type": effective_type.value,
            "rows": len(df),
            "date_range": {"start": str(start_date), "end": str(end_date)},
            "coverage": response.coverage.value,
            "gaps": [g.model_dump() for g in response.gaps],
            "null_counts": df.isnull().sum().to_dict() if not df.empty else {},
        }

        if not df.empty and "close" in df.columns:
            close = pd.to_numeric(df["close"], errors="coerce")
            pct_change = close.pct_change().abs()
            audit_result["price_anomalies"] = {
                "max_daily_move_pct": float(pct_change.max() * 100) if not pct_change.empty else 0,
                "days_over_10pct_move": int((pct_change > 0.10).sum()),
            }

        _output_json(audit_result)
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        raise typer.Exit(code=1)


# ── Options-specific Commands ──────────────────────────────────────────────


@app.command(name="options-chain")
def options_chain(
    symbol: str = typer.Option(..., "--symbol", "-s", help="Ticker symbol, e.g. SPY"),
    expiration: Optional[str] = typer.Option(None, "--expiration", "-e",
                                              help="Expiration date YYYY-MM-DD (default: latest available)"),
    option_type: Optional[str] = typer.Option(None, "--type", "-t",
                                               help="Filter by option type: call | put"),
    snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date",
                                                 help="Snapshot date YYYY-MM-DD (default: latest)"),
    format: str = typer.Option("json", "--format", help="Output format: json | csv"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Query stored options chain for a symbol.

    Returns all strikes from the latest stored snapshot, or a specific snapshot date.
    Greeks (delta, gamma, theta, vega, rho) and IV are populated when data was
    fetched from TastyTrade; fields are null for Databento OPRA.PILLAR data.

    Used by strategy agents to build spreads, evaluate premium, and assess IV.

    Examples:
      market-data options-chain --symbol SPY
      market-data options-chain --symbol SPY --expiration 2026-03-20 --type call
      market-data options-chain --symbol SPY --snapshot-date 2026-03-20
      market-data options-chain --symbol SPY --expiration 2026-03-28 --snapshot-date 2026-03-20
    """
    _setup_logging(verbose)

    exp_date = date.fromisoformat(expiration) if expiration else None
    snap_date = date.fromisoformat(snapshot_date) if snapshot_date else None

    try:
        svc = _get_service()
        df = svc.store.query_options_snapshot(
            symbol=symbol.upper(),
            snapshot_date=snap_date,
            expiration_date=exp_date,
            option_type=option_type,
        )

        if df.empty:
            stderr.print(
                f"[yellow]No options data stored for {symbol.upper()}.[/yellow] "
                "Run: market-data get --symbol "
                f"{symbol.upper()} --type options_chain"
            )
            _output_json({"symbol": symbol.upper(), "rows": 0, "data": []})
            return

        schema = list(df.columns)
        rows = df.to_dict("records")
        for row in rows:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()

        payload = {
            "symbol": symbol.upper(),
            "expiration": str(exp_date) if exp_date else "all",
            "option_type": option_type or "all",
            "rows": len(rows),
            "schema": schema,
            "data": rows,
        }

        if format == "csv":
            _output_csv(rows, schema)
        else:
            _output_json(payload)

    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        if verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        raise typer.Exit(code=1)


@app.command(name="iv-rank")
def iv_rank(
    symbol: str = typer.Option(..., "--symbol", "-s", help="Ticker symbol, e.g. AAPL"),
    days: int = typer.Option(252, "--days", "-d",
                              help="Lookback window in calendar days (default: 252 ≈ 1 trading year)"),
    force_refresh: bool = typer.Option(False, "--force-refresh", "-f",
                                        help="Recompute today's rank/percentile from the full lookback history"),
    format: str = typer.Option("json", "--format", help="Output format: json | csv"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Query historical IV rank for a symbol.

    IV rank measures where current implied volatility sits relative to its
    52-week range. Values near 100 indicate elevated IV (sell premium);
    values near 0 indicate depressed IV (buy premium).

    Use --force-refresh after running iv-rank-backfill to recompute today's
    rank/percentile against the newly populated history.

    Used by strategy agents for all volatility-based entry/exit decisions.

    Examples:
      market-data iv-rank --symbol AAPL
      market-data iv-rank --symbol SPY --days 504  # 2-year lookback
      market-data iv-rank --symbol AAPL --force-refresh  # recompute after backfill
    """
    _setup_logging(verbose)

    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    try:
        svc = _get_service()
        df = svc.get_iv_rank(
            symbol=symbol.upper(),
            lookback_days=days,
            force_refresh=force_refresh,
        )

        if df.empty:
            stderr.print(
                f"[yellow]No IV rank data available for {symbol.upper()}.[/yellow] "
                "Check that FINNHUB_API_KEY is set and the symbol has listed options."
            )
            _output_json({"symbol": symbol.upper(), "rows": 0, "data": []})
            return

        schema = list(df.columns)
        rows = df.to_dict("records")
        for row in rows:
            for k, v in row.items():
                if hasattr(v, "isoformat"):
                    row[k] = v.isoformat()

        # Compute summary stats
        current = rows[-1] if rows else {}
        payload = {
            "symbol": symbol.upper(),
            "lookback_days": days,
            "current_iv_rank": current.get("iv_rank"),
            "current_iv_percentile": current.get("iv_percentile"),
            "current_iv": current.get("current_iv"),
            "rows": len(rows),
            "schema": schema,
            "data": rows,
        }

        if format == "csv":
            _output_csv(rows, schema)
        else:
            _output_json(payload)

    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        if verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        raise typer.Exit(code=1)


@app.command(name="max-pain")
def max_pain(
    symbol: str = typer.Option(..., "--symbol", "-s", help="Ticker symbol, e.g. SPY"),
    expiration: str = typer.Option(..., "--expiration", "-e", help="Expiration date YYYY-MM-DD"),
    snapshot_date: Optional[str] = typer.Option(None, "--snapshot-date",
                                                 help="Snapshot date YYYY-MM-DD (default: latest)"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Compute max pain price for an options expiration.

    Max pain is the strike price where total option holder loss is maximized
    (i.e., where option writers profit most). Calculated from open interest
    across all strikes for the given expiration.

    Used by max-pain and gamma-based strategy agents.

    Examples:
      market-data max-pain --symbol SPY --expiration 2026-03-20
      market-data max-pain --symbol SPY --expiration 2026-03-20 --snapshot-date 2026-03-20
    """
    _setup_logging(verbose)

    exp_date = date.fromisoformat(expiration)
    snap_date = date.fromisoformat(snapshot_date) if snapshot_date else None

    try:
        svc = _get_service()
        result = svc.store.compute_max_pain(
            symbol=symbol.upper(),
            expiration_date=exp_date,
            snapshot_date=snap_date,
        )

        if result.get("max_pain_price") is None:
            stderr.print(
                f"[yellow]No options data for {symbol.upper()} expiring {expiration}.[/yellow] "
                "Run: market-data get --symbol "
                f"{symbol.upper()} --type options_chain first."
            )
            _output_json({"symbol": symbol.upper(), "expiration": expiration,
                          "max_pain_price": None, "strikes": [],
                          "call_oi": [], "put_oi": [], "total_pain": []})
            return

        result["symbol"] = symbol.upper()
        result["expiration"] = expiration
        _output_json(result)

    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        if verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        raise typer.Exit(code=1)


# ── Validate Command ───────────────────────────────────────────────────────


@app.command()
def validate(
    date_str: Optional[str] = typer.Option(None, "--date", help="Date to validate YYYY-MM-DD (default: most recent trading day)"),
    report_dir: str = typer.Option("data/validation", "--report-dir", help="Directory to save JSON reports"),
    json_only: bool = typer.Option(False, "--json-only", help="Skip Rich table; only write JSON report"),
    skip_ohlcv: bool = typer.Option(False, "--skip-ohlcv", help="Skip OHLCV accuracy checks"),
    skip_fundamentals: bool = typer.Option(False, "--skip-fundamentals", help="Skip fundamentals accuracy checks"),
    skip_options: bool = typer.Option(False, "--skip-options", help="Skip Databento options integrity checks"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Cross-validate MDS data accuracy against independent reference sources.

    Compares Alpha Vantage OHLCV and fundamentals against yfinance, and
    checks Databento OPRA.PILLAR options structural integrity.

    Exit codes: 0=all PASS, 1=WARN, 2=FAIL, 3=ERROR

    Examples:
      market-data validate
      market-data validate --date 2026-03-20
      market-data validate --skip-options --json-only
      market-data validate --date 2026-03-20 --report-dir /tmp/reports
    """
    import subprocess
    import os

    _setup_logging(verbose)

    script = os.path.join(os.path.dirname(os.path.dirname(__file__)), "scripts", "validate_accuracy.py")
    if not os.path.exists(script):
        stderr.print(f"[red]Validation script not found:[/red] {script}")
        raise typer.Exit(code=3)

    cmd = [sys.executable, script, "--report-dir", report_dir]
    if date_str:
        cmd += ["--date", date_str]
    if json_only:
        cmd.append("--json-only")
    if skip_ohlcv:
        cmd.append("--skip-ohlcv")
    if skip_fundamentals:
        cmd.append("--skip-fundamentals")
    if skip_options:
        cmd.append("--skip-options")

    result = subprocess.run(cmd)
    raise typer.Exit(code=result.returncode)


@app.command(name="iv-rank-backfill")
def iv_rank_backfill(
    symbol: str = typer.Option(..., "--symbol", "-s", help="Ticker symbol, e.g. SPY"),
    start: str = typer.Option(..., "--start", help="Start date YYYY-MM-DD"),
    end: Optional[str] = typer.Option(None, "--end", help="End date YYYY-MM-DD (default: today)"),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip cost warning confirmation"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """
    Backfill historical IV rank for a symbol using Databento OPRA.PILLAR.

    For each trading day in the range, fetches the options chain, computes ATM
    implied volatility via Black-Scholes, and stores iv_rank + iv_percentile in
    the local database. Skips dates already stored (safe to re-run after interrupt).

    COST WARNING: Each date requires 2 Databento API calls (definition + ohlcv-1d).
    A 1-year backfill (~252 trading days) ~= 504 API calls. Check your Databento
    plan before proceeding. Use --yes to skip the confirmation prompt.

    Requires DATABENTO_API_KEY in your .env file.

    Examples:
      market-data iv-rank-backfill --symbol SPY --start 2020-01-01
      market-data iv-rank-backfill --symbol SPY --start 2019-01-01 --end 2024-12-31 --yes
    """
    _setup_logging(verbose)

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end) if end else date.today()

    if end_date < start_date:
        stderr.print("[red]Error:[/red] --end must be >= --start")
        raise typer.Exit(code=1)

    try:
        svc = _get_service()
        provider = svc.router.get("databento")
        if provider is None:
            stderr.print(
                "[red]Error:[/red] Databento provider not initialized. "
                "Set DATABENTO_API_KEY in market-data-service/.env"
            )
            raise typer.Exit(code=1)

        # Estimate cost before proceeding
        from market_data.iv_backfill import trading_days
        days = trading_days(start_date, end_date)
        existing_df = svc.store.query_iv_rank_history(symbol.upper(), start=start_date, end=end_date)
        already_stored = len(existing_df) if not existing_df.empty else 0
        to_fetch = len(days) - already_stored

        stderr.print(f"\n[bold]IV Rank Backfill — {symbol.upper()}[/bold]")
        stderr.print(f"  Date range:      {start_date} -> {end_date}")
        stderr.print(f"  Trading days:    {len(days)}")
        stderr.print(f"  Already stored:  {already_stored}")
        stderr.print(f"  To fetch:        {to_fetch}")
        stderr.print(f"  Est. API calls:  ~{to_fetch * 2} (2 per day: definition + ohlcv-1d)")

        if to_fetch == 0:
            stderr.print("\n[green]All dates already stored. Nothing to do.[/green]")
            _output_json({"symbol": symbol.upper(), "processed": 0, "skipped": len(days)})
            return

        if not yes:
            stderr.print(
                "\n[yellow]Each Databento API call is billed by data volume. "
                "Ensure your plan covers this usage.[/yellow]"
            )
            confirm = typer.confirm("Proceed with backfill?")
            if not confirm:
                stderr.print("Aborted.")
                raise typer.Exit(code=0)

        # Fetch underlying OHLCV for the full range (uses local cache first)
        stderr.print(f"\nFetching underlying OHLCV for {symbol.upper()} ...")
        ohlcv_response = svc.get(
            symbol=symbol.upper(),
            data_type=DataType.OHLCV,
            start=start_date,
            end=end_date,
        )
        underlying_prices: dict[date, float] = {}
        for row in ohlcv_response.data:
            ts = row.get("timestamp") or row.get("date")
            close = row.get("close")
            if ts and close:
                try:
                    d = date.fromisoformat(str(ts)[:10])
                    underlying_prices[d] = float(close)
                except (ValueError, TypeError):
                    pass

        if not underlying_prices:
            stderr.print(
                f"[red]Error:[/red] No OHLCV data for {symbol.upper()} in range. "
                "Run first: market-data get --symbol "
                f"{symbol.upper()} --type ohlcv --start {start_date} --end {end_date}"
            )
            raise typer.Exit(code=1)

        stderr.print(f"  Underlying prices: {len(underlying_prices)} dates loaded")

        # Run the backfill with a Rich progress bar
        from rich.progress import (
            Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn,
        )
        from market_data.iv_backfill import backfill_iv_rank

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[status]}"),
            console=stderr,
            transient=False,
        ) as progress:
            task = progress.add_task(
                f"Backfilling {symbol.upper()} IV rank",
                total=to_fetch,
                status="",
            )

            def on_progress(current, total_days, snap, status):  # noqa: ANN001
                if status.startswith("ok:"):
                    progress.advance(task)
                    progress.update(task, status=f"[green]{snap}[/green] {status[3:]}")

            processed, skipped = backfill_iv_rank(
                symbol=symbol.upper(),
                start_date=start_date,
                end_date=end_date,
                provider=provider,
                store=svc.store,
                underlying_prices=underlying_prices,
                progress_callback=on_progress,
            )

        stderr.print(
            f"\n[green]Backfill complete:[/green] {processed} processed, {skipped} skipped"
        )
        _output_json({
            "symbol":    symbol.upper(),
            "start":     start_date.isoformat(),
            "end":       end_date.isoformat(),
            "processed": processed,
            "skipped":   skipped,
        })

    except typer.Exit:
        raise
    except Exception as e:
        stderr.print(f"[red]Error:[/red] {e}")
        if verbose:
            import traceback
            traceback.print_exc(file=sys.stderr)
        raise typer.Exit(code=1)


# ── Entry Point ────────────────────────────────────────────────────────────

def main() -> None:
    """CLI entry point registered in pyproject.toml."""
    app()


if __name__ == "__main__":
    main()
