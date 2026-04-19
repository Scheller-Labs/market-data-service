"""
tests/integration/test_data_accuracy.py

Integration tests for MDS data accuracy — cross-validates Alpha Vantage OHLCV
and fundamentals against yfinance, and checks Databento OPRA.PILLAR structural
integrity.

These tests call real external APIs and require all MDS_* env vars to be set.
Run with:
    pytest tests/integration/test_data_accuracy.py -m integration -v

Or via the CLI:
    market-data validate --date 2026-03-20
"""

import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest

# Add scripts/ to path so we can import validation helpers
SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))

pytestmark = pytest.mark.integration

# Tolerance thresholds (mirror validate_accuracy.py)
OHLCV_PASS_PCT = 0.20    # |diff| < 0.20% → PASS
OHLCV_WARN_PCT = 1.00    # |diff| < 1.00% → WARN; above → FAIL
VOLUME_WARN_PCT = 25.0   # AV includes extended hours; ±25% acceptable
FUND_PASS_PCT = 1.00     # fundamentals tolerate minor source rounding
FUND_WARN_PCT = 5.00

# Reference date: last completed trading day (Friday if weekend)
def _last_trading_day() -> date:
    d = date.today() - timedelta(days=1)
    while d.weekday() >= 5:  # Saturday=5, Sunday=6
        d -= timedelta(days=1)
    return d


OHLCV_SYMBOLS = ["AAPL", "SPY", "GOOGL", "MSFT", "TSLA"]
FUNDAMENTALS_SYMBOLS = ["AAPL", "GOOGL", "MSFT"]
OPTIONS_SYMBOLS = ["SPY", "AAPL"]


# ---------------------------------------------------------------------------
# OHLCV accuracy
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def ohlcv_checks():
    """Run OHLCV cross-validation once for the module."""
    from validate_accuracy import _run_ohlcv_checks
    target = _last_trading_day()
    return _run_ohlcv_checks(target)


@pytest.mark.parametrize("symbol", OHLCV_SYMBOLS)
def test_ohlcv_close_accuracy(ohlcv_checks, symbol):
    """Close price from Alpha Vantage must agree with yfinance within 1%."""
    close_checks = [
        c for c in ohlcv_checks
        if c.symbol == symbol and c.metric == "close (adj)"
    ]
    assert close_checks, f"No close check found for {symbol}"
    check = close_checks[0]

    if check.status == "ERROR":
        pytest.skip(f"{symbol} close check errored: {check.note}")

    assert check.diff_pct is not None
    assert abs(check.diff_pct) < OHLCV_WARN_PCT, (
        f"{symbol} close diff {check.diff_pct:+.4f}% exceeds {OHLCV_WARN_PCT}% tolerance. "
        f"AV={check.ref_value}, yf={check.prv_value}"
    )


@pytest.mark.parametrize("symbol", OHLCV_SYMBOLS)
def test_ohlcv_volume_within_extended_hours_tolerance(ohlcv_checks, symbol):
    """Volume diff between AV and yfinance must be within ±25% (AV includes extended hours)."""
    vol_checks = [
        c for c in ohlcv_checks
        if c.symbol == symbol and c.metric == "volume"
    ]
    assert vol_checks, f"No volume check found for {symbol}"
    check = vol_checks[0]

    if check.status == "ERROR":
        pytest.skip(f"{symbol} volume check errored: {check.note}")

    assert check.diff_pct is not None
    assert abs(check.diff_pct) < VOLUME_WARN_PCT, (
        f"{symbol} volume diff {check.diff_pct:+.4f}% exceeds {VOLUME_WARN_PCT}% tolerance. "
        f"AV={check.ref_value}, yf={check.prv_value}"
    )


# ---------------------------------------------------------------------------
# Fundamentals accuracy
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def fundamentals_checks():
    """Run fundamentals cross-validation once for the module."""
    from validate_accuracy import _run_fundamentals_checks
    return _run_fundamentals_checks()


FUNDAMENTALS_METRICS = ["P/E Ratio", "EPS", "Market Cap", "ROE"]


@pytest.mark.parametrize("symbol", FUNDAMENTALS_SYMBOLS)
@pytest.mark.parametrize("metric", FUNDAMENTALS_METRICS)
def test_fundamentals_accuracy(fundamentals_checks, symbol, metric):
    """Fundamentals from Alpha Vantage must agree with yfinance within 5%."""
    checks = [
        c for c in fundamentals_checks
        if c.symbol == symbol and c.metric == metric
    ]
    if not checks:
        pytest.skip(f"No {metric} check for {symbol} (may be N/A)")
    check = checks[0]

    if check.status == "ERROR":
        pytest.skip(f"{symbol} {metric} errored: {check.note}")

    assert check.diff_pct is not None
    assert abs(check.diff_pct) < FUND_WARN_PCT, (
        f"{symbol} {metric} diff {check.diff_pct:+.4f}% exceeds {FUND_WARN_PCT}% tolerance. "
        f"AV={check.ref_value}, yf={check.prv_value}"
    )


# ---------------------------------------------------------------------------
# Databento options structural integrity
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def options_checks():
    """Run Databento options structural checks once for the module."""
    from validate_accuracy import _run_options_checks
    # Use a recent completed trading Friday for options checks
    target = _last_trading_day()
    return _run_options_checks(target)


@pytest.mark.parametrize("symbol", OPTIONS_SYMBOLS)
def test_options_has_contracts(options_checks, symbol):
    """Databento OPRA snapshot must return at least 100 contracts per symbol."""
    checks = [
        c for c in options_checks
        if c.symbol == symbol and c.metric == "total_contracts"
    ]
    assert checks, f"No total_contracts check for {symbol}"
    check = checks[0]

    if check.status == "ERROR":
        pytest.skip(f"{symbol} options data unavailable: {check.note}")

    assert check.prv_value is not None
    assert int(check.prv_value) >= 100, (
        f"{symbol} only {check.prv_value} contracts in Databento snapshot"
    )


@pytest.mark.parametrize("symbol", OPTIONS_SYMBOLS)
def test_options_call_put_symmetry(options_checks, symbol):
    """Call/put contract counts must be equal (symmetry error = 0%)."""
    checks = [
        c for c in options_checks
        if c.symbol == symbol and c.metric == "call/put symmetry error"
    ]
    assert checks, f"No call/put symmetry check for {symbol}"
    check = checks[0]

    if check.status == "ERROR":
        pytest.skip(f"{symbol} symmetry check errored: {check.note}")

    assert check.diff_pct is not None
    assert check.diff_pct == 0.0, (
        f"{symbol} call/put symmetry error {check.diff_pct:.2f}% — "
        "unequal call/put counts indicate a data pipeline issue"
    )


@pytest.mark.parametrize("symbol", OPTIONS_SYMBOLS)
def test_options_eod_price_coverage(options_checks, symbol):
    """At least 40% of options contracts must have a non-null EOD close (last) price."""
    checks = [
        c for c in options_checks
        if c.symbol == symbol and c.metric == "EOD price coverage"
    ]
    assert checks, f"No EOD price coverage check for {symbol}"
    check = checks[0]

    if check.status == "ERROR":
        pytest.skip(f"{symbol} EOD coverage check errored: {check.note}")

    assert check.prv_value is not None
    assert float(check.prv_value) >= 40.0, (
        f"{symbol} only {check.prv_value:.1f}% of contracts have EOD price — "
        "expected ≥40% (deep OTM contracts legitimately have no trade)"
    )


# ---------------------------------------------------------------------------
# CLI validate command smoke test
# ---------------------------------------------------------------------------

def test_cli_validate_command_runs():
    """market-data validate --skip-options completes without FAIL exit code."""
    import subprocess
    result = subprocess.run(
        ["market-data", "validate", "--skip-options", "--json-only",
         "--date", str(_last_trading_day())],
        capture_output=True,
        text=True,
        timeout=300,
    )
    # Exit 0 = PASS, 1 = WARN — both are acceptable; 2 = FAIL or 3 = ERROR are not
    assert result.returncode in (0, 1), (
        f"market-data validate exited {result.returncode}\n"
        f"stdout: {result.stdout[:500]}\n"
        f"stderr: {result.stderr[:500]}"
    )
