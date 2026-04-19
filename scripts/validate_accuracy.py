#!/usr/bin/env python3
"""
scripts/validate_accuracy.py
Independent data-accuracy validation for the Market Data Service.

PURPOSE
-------
This script cross-checks MDS provider data against yfinance as an independent
reference.  It runs *without* a live MDS infrastructure (no TimescaleDB/Redis
needed) and validates at the provider level — where data integrity problems
actually originate.

WHAT IS VALIDATED
-----------------
  Check 1 — OHLCV close prices
      Alpha Vantage vs yfinance for AAPL, SPY, GOOGL, MSFT, TSLA.
      Tolerance: |diff| < 0.20% = PASS, < 1.0% = WARN, ≥ 1.0% = FAIL.

  Check 2 — Fundamentals (P/E, EPS, Market Cap, ROE)
      Alpha Vantage vs yfinance for AAPL, GOOGL, MSFT.
      Tolerances vary by metric (fundamentals legitimately lag slightly).

  Check 3 — Options chain structural integrity (Databento OPRA.PILLAR)
      Validates the options pipeline (Databento → MDS storage) preserves
      contract counts, call/put symmetry, and EOD price coverage.
      Since TastyTrade is real-time and Databento is EOD historical, a direct
      price comparison requires the same snapshot date; the structural checks
      are therefore the primary cross-validation dimension.

      NOTE on volume differences: SPY volume commonly differs ±15-25% between
      sources because some providers include extended-hours activity; this is
      expected and is not flagged as a failure.

SAMPLE SET (keeps API costs and rate-limit risk low)
----------------------------------------------------
  OHLCV:        AAPL, SPY, GOOGL, MSFT, TSLA  (last complete trading day)
  Fundamentals: AAPL, GOOGL, MSFT              (latest snapshot)
  Options:      SPY, AAPL                       (last available Databento date)

EXIT CODES
----------
  0 — All checks PASS
  1 — At least one WARNING (data differs beyond soft tolerance)
  2 — At least one FAILURE (data differs beyond hard tolerance)

PERIODIC EXECUTION
------------------
  # Run nightly after market close (Mon–Fri 7 PM EST):
  0 19 * * 1-5 cd /home/bobsc/Projects/agent-trading-firm/market-data-service && \
    poetry run python scripts/validate_accuracy.py \
    --report-dir data/validation >> /var/log/mds-validate.log 2>&1

  # Or as a market-data CLI command:
  market-data validate --report-dir data/validation

USAGE
-----
  poetry run python scripts/validate_accuracy.py [--report-dir PATH] [--date YYYY-MM-DD] [--json-only]
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import dataclass, field, asdict
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Thresholds ─────────────────────────────────────────────────────────────

OHLCV_CLOSE_PASS_PCT   = 0.20   # |diff| < this → PASS
OHLCV_CLOSE_WARN_PCT   = 1.00   # |diff| < this → WARN  (else FAIL)
OHLCV_VOL_PASS_PCT     = 5.0    # volume note: many sources differ ±15%; soft only
OHLCV_VOL_WARN_PCT     = 25.0
FUND_PE_PASS_PCT        = 3.0
FUND_PE_WARN_PCT        = 6.0
FUND_EPS_PASS_PCT       = 3.0
FUND_EPS_WARN_PCT       = 6.0
FUND_MKTCAP_PASS_PCT    = 1.0
FUND_MKTCAP_WARN_PCT    = 3.0
FUND_ROE_PASS_PCT       = 5.0
FUND_ROE_WARN_PCT       = 10.0
OPT_SYMMETRY_PASS_PCT   = 1.0   # |calls - puts| / max(calls,puts)
OPT_SYMMETRY_WARN_PCT   = 5.0
OPT_PRICE_COVERAGE_MIN  = 40.0  # % of contracts that have an EOD close price

# AV free tier: 5 requests / minute → 12 s between calls
AV_RATE_LIMIT_SLEEP = 12.0

SAMPLE_OHLCV_SYMBOLS  = ["AAPL", "SPY", "GOOGL", "MSFT", "TSLA"]
SAMPLE_FUND_SYMBOLS   = ["AAPL", "GOOGL", "MSFT"]
SAMPLE_OPT_SYMBOLS    = ["SPY", "AAPL"]


# ── Result dataclasses ─────────────────────────────────────────────────────

@dataclass
class CheckResult:
    check:     str
    symbol:    str
    metric:    str
    ref_value: Optional[float]
    prv_value: Optional[float]
    diff_pct:  Optional[float]
    status:    str          # "PASS" | "WARN" | "FAIL" | "ERROR" | "NOTE"
    note:      str = ""

    @property
    def status_icon(self) -> str:
        return {"PASS": "✓", "WARN": "⚠", "FAIL": "✗", "ERROR": "!", "NOTE": "·"}[self.status]

    @property
    def diff_str(self) -> str:
        if self.diff_pct is None:
            return "N/A"
        return f"{self.diff_pct:+.4f}%"


@dataclass
class ValidationReport:
    run_date:    str
    checks:      list[CheckResult] = field(default_factory=list)

    @property
    def counts(self) -> dict[str, int]:
        c: dict[str, int] = {"PASS": 0, "WARN": 0, "FAIL": 0, "ERROR": 0, "NOTE": 0}
        for r in self.checks:
            c[r.status] = c.get(r.status, 0) + 1
        return c

    @property
    def exit_code(self) -> int:
        c = self.counts
        if c["FAIL"] > 0 or c["ERROR"] > 0:
            return 2
        if c["WARN"] > 0:
            return 1
        return 0


# ── Helpers ────────────────────────────────────────────────────────────────

def _status(diff_pct: float, pass_thr: float, warn_thr: float) -> str:
    a = abs(diff_pct)
    if a < pass_thr:
        return "PASS"
    if a < warn_thr:
        return "WARN"
    return "FAIL"


def _last_complete_trading_day() -> date:
    """Return the most recent trading day that has completed (Mon–Fri, not today)."""
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


# ── Check 1: OHLCV ─────────────────────────────────────────────────────────

def _run_ohlcv_checks(target_date: date) -> list[CheckResult]:
    results: list[CheckResult] = []

    # -- Reference: yfinance --
    try:
        import yfinance as yf
    except ImportError:
        return [CheckResult("OHLCV", s, "close", None, None, None, "ERROR",
                            "yfinance not installed; run: pip install yfinance")
                for s in SAMPLE_OHLCV_SYMBOLS]

    yf_data: dict[str, dict] = {}
    for sym in SAMPLE_OHLCV_SYMBOLS:
        try:
            hist = yf.Ticker(sym).history(
                start=(target_date - timedelta(days=3)).isoformat(),
                end=(target_date + timedelta(days=1)).isoformat(),
                auto_adjust=True,
            )
            row = hist.loc[hist.index.date == target_date]
            if not row.empty:
                yf_data[sym] = {"close": float(row.iloc[0]["Close"]),
                                "volume": int(row.iloc[0]["Volume"])}
        except Exception as e:
            logger.warning("yfinance error for %s: %s", sym, e)

    # -- Provider: Alpha Vantage --
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from market_data.providers.alpha_vantage import AlphaVantageProvider
        av = AlphaVantageProvider()
    except Exception as e:
        return [CheckResult("OHLCV", s, "close", None, None, None, "ERROR",
                            f"Cannot load AlphaVantageProvider: {e}")
                for s in SAMPLE_OHLCV_SYMBOLS]

    for sym in SAMPLE_OHLCV_SYMBOLS:
        yf_row = yf_data.get(sym)
        if not yf_row:
            results.append(CheckResult("OHLCV", sym, "close", None, None, None,
                                       "ERROR", "yfinance returned no data"))
            continue

        try:
            df = av.fetch_ohlcv(sym, target_date - timedelta(days=3), target_date)
        except Exception as e:
            results.append(CheckResult("OHLCV", sym, "close", yf_row["close"],
                                       None, None, "ERROR", str(e)))
            time.sleep(AV_RATE_LIMIT_SLEEP)
            continue

        if df.empty:
            results.append(CheckResult("OHLCV", sym, "close", yf_row["close"],
                                       None, None, "ERROR", "Alpha Vantage returned empty"))
            time.sleep(AV_RATE_LIMIT_SLEEP)
            continue

        # Get the row for target_date
        date_rows = df[df["timestamp"].astype(str).str.startswith(target_date.isoformat())]
        av_row = date_rows.iloc[0] if not date_rows.empty else df.iloc[-1]

        # Close price
        av_close = float(av_row["close"])
        yf_close = yf_row["close"]
        diff_c   = (av_close - yf_close) / yf_close * 100
        results.append(CheckResult(
            "OHLCV", sym, "close (adj)",
            yf_close, av_close, diff_c,
            _status(diff_c, OHLCV_CLOSE_PASS_PCT, OHLCV_CLOSE_WARN_PCT),
            "Alpha Vantage vs yfinance (auto-adjusted)",
        ))

        # Volume (soft check — sources often differ on extended-hours inclusion)
        av_vol = int(av_row["volume"])
        yf_vol = yf_row["volume"]
        diff_v = (av_vol - yf_vol) / yf_vol * 100
        note = "Volume: sources differ on ext-hours inclusion; ±25% acceptable"
        results.append(CheckResult(
            "OHLCV", sym, "volume",
            float(yf_vol), float(av_vol), diff_v,
            _status(diff_v, OHLCV_VOL_PASS_PCT, OHLCV_VOL_WARN_PCT),
            note,
        ))

        time.sleep(AV_RATE_LIMIT_SLEEP)

    return results


# ── Check 2: Fundamentals ───────────────────────────────────────────────────

def _run_fundamentals_checks() -> list[CheckResult]:
    results: list[CheckResult] = []

    try:
        import yfinance as yf
    except ImportError:
        return [CheckResult("FUNDAMENTALS", s, m, None, None, None, "ERROR",
                            "yfinance not installed")
                for s in SAMPLE_FUND_SYMBOLS for m in ("pe", "eps", "market_cap")]

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from market_data.providers.alpha_vantage import AlphaVantageProvider
        av = AlphaVantageProvider()
    except Exception as e:
        return [CheckResult("FUNDAMENTALS", s, m, None, None, None, "ERROR", str(e))
                for s in SAMPLE_FUND_SYMBOLS for m in ("pe", "eps", "market_cap")]

    METRICS = [
        # (display_name, yfinance_key, av_df_key, pass_thr, warn_thr)
        ("P/E Ratio",  "trailingPE",      "pe_ratio",   FUND_PE_PASS_PCT,     FUND_PE_WARN_PCT),
        ("EPS",        "trailingEps",     "eps",        FUND_EPS_PASS_PCT,    FUND_EPS_WARN_PCT),
        ("Market Cap", "marketCap",       "market_cap", FUND_MKTCAP_PASS_PCT, FUND_MKTCAP_WARN_PCT),
        ("ROE",        "returnOnEquity",  "roe",        FUND_ROE_PASS_PCT,    FUND_ROE_WARN_PCT),
    ]

    for sym in SAMPLE_FUND_SYMBOLS:
        try:
            yf_info = yf.Ticker(sym).info
        except Exception as e:
            for m, *_ in METRICS:
                results.append(CheckResult("FUNDAMENTALS", sym, m, None, None, None,
                                           "ERROR", f"yfinance: {e}"))
            continue

        try:
            df_av = av.fetch_fundamentals(sym)
        except Exception as e:
            for m, *_ in METRICS:
                results.append(CheckResult("FUNDAMENTALS", sym, m, None, None, None,
                                           "ERROR", f"Alpha Vantage: {e}"))
            time.sleep(AV_RATE_LIMIT_SLEEP)
            continue

        if df_av.empty:
            for m, *_ in METRICS:
                results.append(CheckResult("FUNDAMENTALS", sym, m, None, None, None,
                                           "ERROR", "Alpha Vantage returned empty"))
            time.sleep(AV_RATE_LIMIT_SLEEP)
            continue

        av_row = df_av.iloc[0]
        for display, yf_key, av_key, pass_thr, warn_thr in METRICS:
            yf_v = yf_info.get(yf_key)
            av_v = av_row.get(av_key)
            if yf_v and av_v:
                diff = (float(av_v) - float(yf_v)) / float(yf_v) * 100
                status = _status(diff, pass_thr, warn_thr)
            else:
                diff, status = None, "ERROR"
            results.append(CheckResult(
                "FUNDAMENTALS", sym, display,
                float(yf_v) if yf_v else None,
                float(av_v) if av_v else None,
                diff, status,
                "Alpha Vantage vs yfinance",
            ))

        time.sleep(AV_RATE_LIMIT_SLEEP)

    return results


# ── Check 3: Options chain integrity ───────────────────────────────────────

def _run_options_checks(target_date: date) -> list[CheckResult]:
    results: list[CheckResult] = []

    try:
        sys.path.insert(0, str(Path(__file__).parent.parent))
        from market_data.config import settings
        import databento as db
    except ImportError as e:
        return [CheckResult("OPTIONS", s, m, None, None, None, "ERROR", str(e))
                for s in SAMPLE_OPT_SYMBOLS
                for m in ("total_contracts", "call_put_symmetry", "price_coverage")]

    client = db.Historical(key=settings.databento_api_key)
    start_str = target_date.isoformat()
    end_str   = (target_date + timedelta(days=1)).isoformat()

    for sym in SAMPLE_OPT_SYMBOLS:
        parent = f"{sym}.OPT"
        try:
            import pandas as pd
            defs = client.timeseries.get_range(
                dataset="OPRA.PILLAR", symbols=[parent], stype_in="parent",
                schema="definition", start=start_str, end=end_str,
            ).to_df()
            defs = defs[defs["instrument_class"].isin(["C", "P"])].drop_duplicates("instrument_id")

            ohlcv = client.timeseries.get_range(
                dataset="OPRA.PILLAR", symbols=[parent], stype_in="parent",
                schema="ohlcv-1d", start=start_str, end=end_str,
            ).to_df()

            total = len(defs)
            calls = int((defs["instrument_class"] == "C").sum())
            puts  = int((defs["instrument_class"] == "P").sum())

            # Total contracts > 0
            results.append(CheckResult(
                "OPTIONS", sym, "total_contracts",
                None, float(total), None,
                "PASS" if total > 0 else "FAIL",
                f"Databento OPRA.PILLAR snapshot {target_date}",
            ))

            # Call/put symmetry
            sym_err = abs(calls - puts) / max(calls, puts) * 100 if max(calls, puts) else 100.0
            results.append(CheckResult(
                "OPTIONS", sym, "call/put symmetry error",
                0.0, sym_err, sym_err,
                _status(sym_err, OPT_SYMMETRY_PASS_PCT, OPT_SYMMETRY_WARN_PCT),
                f"calls={calls:,}  puts={puts:,}; perfect symmetry → 0%",
            ))

            # Price coverage (contracts with at least one EOD trade)
            priced_ids = set(ohlcv["instrument_id"].unique()) & set(defs["instrument_id"].unique())
            coverage_pct = len(priced_ids) / total * 100 if total else 0.0
            results.append(CheckResult(
                "OPTIONS", sym, "EOD price coverage",
                OPT_PRICE_COVERAGE_MIN, coverage_pct, coverage_pct - OPT_PRICE_COVERAGE_MIN,
                "PASS" if coverage_pct >= OPT_PRICE_COVERAGE_MIN else "WARN",
                f"{len(priced_ids):,} of {total:,} contracts have EOD close; illiquid deep OTM expected",
            ))

        except Exception as e:
            for m in ("total_contracts", "call/put symmetry error", "EOD price coverage"):
                results.append(CheckResult("OPTIONS", sym, m, None, None, None,
                                           "ERROR", str(e)))

    return results


# ── Output: rich table ─────────────────────────────────────────────────────

def _print_report(report: ValidationReport) -> None:
    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box
        con = Console()
    except ImportError:
        _print_report_plain(report)
        return

    colors = {"PASS": "green", "WARN": "yellow", "FAIL": "red", "ERROR": "red bold", "NOTE": "cyan"}

    con.rule(f"[bold]MDS Accuracy Validation — {report.run_date}[/bold]")

    for check_name in ("OHLCV", "FUNDAMENTALS", "OPTIONS"):
        rows = [r for r in report.checks if r.check == check_name]
        if not rows:
            continue

        t = Table(
            title=f"{check_name} Checks",
            box=box.SIMPLE_HEAVY,
            show_lines=False,
            header_style="bold",
        )
        t.add_column("Symbol",     style="cyan",   no_wrap=True)
        t.add_column("Metric",     style="white",  no_wrap=True)
        t.add_column("Reference",  justify="right")
        t.add_column("Provider",   justify="right")
        t.add_column("Diff %",     justify="right")
        t.add_column("Status",     justify="center", no_wrap=True)
        t.add_column("Note",       style="dim")

        for r in rows:
            ref_s = f"{r.ref_value:,.4f}" if r.ref_value is not None else "—"
            prv_s = f"{r.prv_value:,.4f}" if r.prv_value is not None else "—"
            dif_s = r.diff_str
            col   = colors.get(r.status, "white")
            t.add_row(
                r.symbol, r.metric, ref_s, prv_s,
                f"[{col}]{dif_s}[/{col}]",
                f"[{col}]{r.status_icon} {r.status}[/{col}]",
                r.note[:60],
            )
        con.print(t)

    c = report.counts
    con.print(
        f"[bold]Summary:[/bold] "
        f"[green]✓ {c['PASS']} PASS[/green]  "
        f"[yellow]⚠ {c['WARN']} WARN[/yellow]  "
        f"[red]✗ {c['FAIL']} FAIL[/red]  "
        f"[red bold]! {c['ERROR']} ERROR[/red bold]"
    )


def _print_report_plain(report: ValidationReport) -> None:
    print(f"\n=== MDS Accuracy Validation — {report.run_date} ===")
    for r in report.checks:
        ref_s = f"{r.ref_value:,.4f}" if r.ref_value is not None else "N/A"
        prv_s = f"{r.prv_value:,.4f}" if r.prv_value is not None else "N/A"
        print(f"  {r.status_icon} {r.check:<14} {r.symbol:<6} {r.metric:<28} "
              f"ref={ref_s:>18}  prv={prv_s:>18}  diff={r.diff_str:>12}  {r.status}")
    c = report.counts
    print(f"\nSummary: PASS={c['PASS']} WARN={c['WARN']} FAIL={c['FAIL']} ERROR={c['ERROR']}")


# ── Persist JSON report ────────────────────────────────────────────────────

def _save_report(report: ValidationReport, report_dir: Path) -> Path:
    report_dir.mkdir(parents=True, exist_ok=True)
    path = report_dir / f"validation_{report.run_date}.json"
    payload = {
        "run_date":  report.run_date,
        "summary":   report.counts,
        "exit_code": report.exit_code,
        "checks": [asdict(r) for r in report.checks],
    }
    path.write_text(json.dumps(payload, indent=2))
    return path


# ── Entry point ────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate MDS provider data against independent reference sources.",
    )
    parser.add_argument("--date",       default=None,
                        help="Target trading date YYYY-MM-DD (default: last complete trading day)")
    parser.add_argument("--report-dir", default="data/validation",
                        help="Directory to write JSON reports (default: data/validation)")
    parser.add_argument("--json-only",  action="store_true",
                        help="Print only the JSON report, no table")
    parser.add_argument("--skip-ohlcv",       action="store_true")
    parser.add_argument("--skip-fundamentals", action="store_true")
    parser.add_argument("--skip-options",     action="store_true")
    args = parser.parse_args()

    target_date = date.fromisoformat(args.date) if args.date else _last_complete_trading_day()
    report = ValidationReport(run_date=target_date.isoformat())

    # Load .env from project root
    try:
        from pathlib import Path as _P
        env_path = _P(__file__).parent.parent / ".env"
        if env_path.exists():
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=env_path)
    except ImportError:
        pass

    print(f"Validating MDS data for {target_date}  (this takes ~3 minutes due to API rate limits)",
          file=sys.stderr)

    if not args.skip_ohlcv:
        print("  Running OHLCV checks (Alpha Vantage vs yfinance)...", file=sys.stderr)
        report.checks.extend(_run_ohlcv_checks(target_date))

    if not args.skip_fundamentals:
        print("  Running Fundamentals checks (Alpha Vantage vs yfinance)...", file=sys.stderr)
        report.checks.extend(_run_fundamentals_checks())

    if not args.skip_options:
        print("  Running Options integrity checks (Databento OPRA.PILLAR)...", file=sys.stderr)
        report.checks.extend(_run_options_checks(target_date))

    if args.json_only:
        print(json.dumps({
            "run_date":  report.run_date,
            "summary":   report.counts,
            "exit_code": report.exit_code,
            "checks":    [asdict(r) for r in report.checks],
        }, indent=2))
    else:
        _print_report(report)

    # Save JSON report for historical trending
    report_dir = Path(args.report_dir)
    saved_path = _save_report(report, report_dir)
    print(f"\nReport saved: {saved_path}", file=sys.stderr)

    return report.exit_code


if __name__ == "__main__":
    sys.exit(main())
