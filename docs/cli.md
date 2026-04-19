# Market Data Service — CLI Reference

**Version:** 0.1.0
**Entry point:** `market-data`
**Source:** `market_data/cli.py`

---

## Overview

The Market Data Service CLI (`market-data`) is a cache-first, delta-fetch interface for structured financial data. It is designed as the primary data access layer for both automated trading agents and human operators.

### Design Principles

- **Agents first.** All data output goes to **stdout as JSON** (or CSV on request). All human-readable messages (progress, warnings, errors) go to **stderr**. Agents can safely pipe stdout without filtering noise.
- **Cache-first, delta-fetch.** Every `get` call checks local coverage first. Only the missing date ranges are fetched from upstream providers. Identical requests never hit the API twice.
- **Idempotent.** All writes use `ON CONFLICT DO UPDATE` — re-running any command with the same parameters is always safe.
- **Fail-transparent.** When data is unavailable, the response includes a `coverage` field (`complete`, `partial`, or `missing`) and a `gaps` array. Agents can inspect these rather than receiving silent empty results.

### Cache Tiers

```
Request → Redis hot cache (<1ms)
              ↓ miss
          Coverage manifest check (SQLite, ~1ms)
              ↓ complete
          TimescaleDB warm store (<10ms)
              ↓ gaps found
          Provider API fetch (alpha_vantage / finnhub / databento)
              ↓ write-back
          TimescaleDB + Redis + coverage manifest update
              ↓
          Unified DataResponse → stdout
```

---

## Installation

```bash
cd /home/bobsc/Projects/agent-trading-firm/market-data-service

# Install dependencies
poetry install

# Or with the venv directly
.venv/bin/pip install -e .

# Verify
market-data --help
```

### Infrastructure

```bash
# Start TimescaleDB (port 5433) and Redis (port 6380)
docker compose up -d timescaledb redis

# Verify connectivity
market-data health
```

---

## Environment Variables

All settings are read from `.env` in the working directory or from the environment. Environment variables take precedence over the `.env` file.

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMESCALE_URL` | `postgresql://mds:mds_secret@localhost:5433/market_data` | TimescaleDB connection string |
| `REDIS_URL` | `redis://localhost:6380/0` | Redis connection string |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO endpoint for cold archive |
| `MINIO_ACCESS_KEY` | `mds` | MinIO access key |
| `MINIO_SECRET_KEY` | `mds_secret` | MinIO secret key |
| `COVERAGE_DB_PATH` | `/data/manifest/coverage.db` | Local SQLite coverage manifest path |
| `ALPHA_VANTAGE_API_KEY` | _(none)_ | Alpha Vantage API key |
| `FINNHUB_API_KEY` | _(none)_ | Finnhub API key |
| `DATABENTO_API_KEY` | _(none)_ | Databento API key |
| `MDS_LOG_LEVEL` | `INFO` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `MDS_DRY_RUN` | `false` | If true, gap analysis runs but no API calls are made |
| `MDS_MAX_BATCH_WORKERS` | `4` | Default parallel workers for batch fetch |
| `MDS_REQUEST_TIMEOUT` | `30` | Provider HTTP request timeout in seconds |
| `MDS_MAX_RETRIES` | `3` | Provider retry attempts on transient failure |

---

## Global Flags

These flags are accepted by every command:

| Flag | Description |
|------|-------------|
| `--verbose` / `-v` | Emit `DEBUG`-level logs to stderr. Includes tracebacks on error. |
| `--help` | Print command help and exit. |

---

## Data Types

The `--type` option accepts the following values across all commands that support it:

| Value | Description | Primary Provider |
|-------|-------------|-----------------|
| `ohlcv` | End-of-day OHLCV bars | Alpha Vantage |
| `ohlcv_intraday` | Intraday OHLCV (1m, 5m, 15m, 1h) | Alpha Vantage, Finnhub |
| `options_chain` | Full options chain snapshot with Greeks | Finnhub, Databento |
| `fundamentals` | P/E, EPS, revenue, market cap, sector | Alpha Vantage |
| `earnings` | Earnings history with EPS estimates | Alpha Vantage, Finnhub |
| `dividends` | Ex-date, amount, pay date | Alpha Vantage |
| `news_sentiment` | Headlines with sentiment scores | Finnhub |
| `iv_rank` | IV rank history (separate from options chain) | Computed |
| `futures_ohlcv` | Futures OHLCV (ES, NQ, etc.) | Databento |
| `tick` | Raw tick data | Databento |

**Default:** `ohlcv` when `--type` is omitted.

---

## Bar Intervals

Used with `--interval` on `get` and `batch` commands:

| Value | Description |
|-------|-------------|
| `1m` | 1-minute bars |
| `5m` | 5-minute bars |
| `15m` | 15-minute bars |
| `1h` | 1-hour bars |
| `4h` | 4-hour bars |
| `1d` | Daily bars _(default)_ |
| `1w` | Weekly bars |

---

## Commands

---

### `get` — Fetch Market Data

The primary data access command. Returns data from local cache when available; fetches only the missing date gaps from upstream providers.

```
market-data get --symbol SYMBOL [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(required)_ | Ticker symbol, e.g. `AAPL`, `SPY` |
| `--type` | `-t` | DataType | `ohlcv` | Data type to fetch (see Data Types table) |
| `--start` | | YYYY-MM-DD | _(computed)_ | Start date. Overrides `--days`. |
| `--end` | | YYYY-MM-DD | today | End date |
| `--days` | `-d` | int | `365` | Number of calendar days back from today (used when `--start` is not set) |
| `--interval` | `-i` | Interval | `1d` | Bar interval (for OHLCV data) |
| `--force-refresh` | `-f` | flag | false | Bypass all caches and re-fetch from provider |
| `--provider` | `-p` | string | _(auto)_ | Force a specific provider name |
| `--format` | | `json`\|`csv` | `json` | Output format |
| `--verbose` | `-v` | flag | false | Enable debug logging to stderr |

**Output (JSON):**

```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "interval": "1d",
  "source": "timescaledb",
  "coverage": "complete",
  "gaps": [],
  "rows": 252,
  "fetched_at": "2026-03-21T19:00:00+00:00",
  "schema": ["timestamp", "symbol", "open", "high", "low", "close", "volume", "adj_close", "provider"],
  "data": [
    {
      "timestamp": "2025-03-21T00:00:00+00:00",
      "symbol": "AAPL",
      "open": 215.50,
      "high": 218.90,
      "low": 214.20,
      "close": 217.30,
      "volume": 62450000,
      "adj_close": 217.30,
      "provider": "alpha_vantage"
    }
  ]
}
```

**`source` field values:**

| Value | Meaning |
|-------|---------|
| `timescaledb` | Entire response served from local DB (full cache hit) |
| `merged` | New gaps were fetched from API and merged with existing local data |
| `api:alpha_vantage` | All data fetched live from Alpha Vantage |
| `api:finnhub` | All data fetched live from Finnhub |
| `cache` | Served from Redis hot cache |

**`coverage` field values:**

| Value | Meaning |
|-------|---------|
| `complete` | All requested dates are present in local store |
| `partial` | Some dates are present; `gaps` array lists the missing ranges |
| `missing` | No data exists locally for this symbol/type/range |

**Examples:**

```bash
# 1 year of AAPL daily OHLCV (default)
market-data get --symbol AAPL

# Explicit date range
market-data get --symbol SPY --type ohlcv --start 2024-01-01 --end 2024-12-31

# 90 days of intraday 1h bars
market-data get --symbol TSLA --type ohlcv_intraday --interval 1h --days 90

# Fundamentals (date range is ignored — returns latest snapshot)
market-data get --symbol AAPL --type fundamentals

# Force re-fetch from provider (bypass cache)
market-data get --symbol AAPL --type ohlcv --days 30 --force-refresh

# CSV output for pipeline processing
market-data get --symbol SPY --type ohlcv --days 365 --format csv

# Force specific provider
market-data get --symbol AAPL --type ohlcv --provider finnhub

# Pipe to jq for field extraction (agent pattern)
market-data get --symbol SPY --days 30 | jq '.data[-1]'

# Extract just close prices
market-data get --symbol AAPL --days 5 | jq '[.data[].close]'

# Check if data is complete before using it
market-data get --symbol AAPL --days 30 | jq '{coverage: .coverage, gaps: .gaps}'
```

**Agent usage pattern:**

```python
import subprocess, json

result = subprocess.run(
    ["market-data", "get", "--symbol", "SPY", "--type", "ohlcv", "--days", "252"],
    capture_output=True, text=True, check=True
)
response = json.loads(result.stdout)

assert response["coverage"] == "complete", f"Missing dates: {response['gaps']}"
prices = {row["timestamp"]: row["close"] for row in response["data"]}
```

---

### `status` — Check Local Coverage

Reports what data is available locally without making any API calls or modifying state. Use this before `get` to inspect what will be fetched.

```
market-data status --symbol SYMBOL [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(required)_ | Ticker symbol |
| `--type` | `-t` | DataType | `ohlcv` | Data type |
| `--start` | | YYYY-MM-DD | _(computed)_ | Start of range to check |
| `--end` | | YYYY-MM-DD | today | End of range to check |
| `--days` | `-d` | int | `365` | Days back from today (when `--start` not set) |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "coverage": "partial",
  "available_ranges": [
    {"start": "2025-01-01", "end": "2025-03-10"},
    {"start": "2025-03-15", "end": "2025-03-21"}
  ],
  "gaps": [
    {"start": "2025-03-11", "end": "2025-03-14"}
  ],
  "total_rows": null
}
```

**Examples:**

```bash
# Check 1 year OHLCV coverage for AAPL
market-data status --symbol AAPL

# Check specific range
market-data status --symbol SPY --start 2024-01-01 --end 2024-12-31

# Agent: check before deciding whether to fetch
market-data status --symbol TSLA --days 90 | jq '.coverage'

# Find all gaps
market-data status --symbol AAPL --days 365 | jq '.gaps'
```

---

### `batch` — Parallel Multi-Symbol Fetch

Fetches data for multiple symbols concurrently using a thread pool. Each symbol is processed independently with the same cache-first, delta-fetch logic as `get`.

```
market-data batch --symbols SYMBOL1,SYMBOL2,... [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbols` | | string | _(required)_ | Comma-separated list of symbols |
| `--type` | `-t` | DataType | `ohlcv` | Data type |
| `--days` | `-d` | int | `365` | Calendar days back from today |
| `--interval` | | Interval | `1d` | Bar interval |
| `--workers` | `-w` | int | `4` | Number of parallel fetch threads |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "requested": ["AAPL", "TSLA", "SPY"],
  "succeeded": ["AAPL", "SPY", "TSLA"],
  "failed": [],
  "results": {
    "AAPL": { "symbol": "AAPL", "rows": 252, "coverage": "complete", ... },
    "TSLA": { "symbol": "TSLA", "rows": 252, "coverage": "complete", ... },
    "SPY":  { "symbol": "SPY",  "rows": 252, "coverage": "complete", ... }
  }
}
```

**Examples:**

```bash
# Fetch 3 ETFs
market-data batch --symbols SPY,QQQ,IWM --type ohlcv --days 365

# 8 workers for a large watchlist
market-data batch --symbols AAPL,MSFT,GOOGL,AMZN,META,NVDA,TSLA,SPY --workers 8

# Intraday bars for a basket
market-data batch --symbols SPY,QQQ --type ohlcv_intraday --interval 15m --days 30

# Agent: identify any failures
market-data batch --symbols AAPL,TSLA,SPY | jq '.failed'

# Extract close for all symbols on a specific date (pipe to jq)
market-data batch --symbols AAPL,MSFT --days 5 | \
  jq '.results | to_entries[] | {symbol: .key, close: .value.data[-1].close}'
```

---

### `warm` — Pre-Populate Cache

Bulk-fetches a list of symbols across multiple data types to ensure the local store is fully populated. Uses the same batch logic as `batch` but iterates over data types as well. Useful for end-of-day ETL jobs or before running backtests.

```
market-data warm --watchlist WATCHLIST [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--watchlist` | `-w` | string | _(required)_ | Path to a file (one symbol per line, `#` comments ignored), or an inline comma-separated list |
| `--days` | `-d` | int | `365` | Days of history to ensure coverage for |
| `--types` | | string | `ohlcv,fundamentals` | Comma-separated data types to warm |
| `--verbose` | `-v` | flag | false | Debug logging |

**Watchlist file format:**

```
# My trading watchlist
AAPL
MSFT
GOOGL
SPY    # S&P 500 ETF
```

**Output (JSON):**

```json
{
  "symbols": ["AAPL", "MSFT", "SPY"],
  "data_types": ["ohlcv", "fundamentals"],
  "results": {
    "ohlcv":        {"succeeded": ["AAPL", "MSFT", "SPY"], "failed": []},
    "fundamentals": {"succeeded": ["AAPL", "MSFT", "SPY"], "failed": []}
  }
}
```

**Examples:**

```bash
# Warm from watchlist file
market-data warm --watchlist watchlist.txt

# Warm inline symbols for specific types
market-data warm --watchlist SPY,QQQ,IWM --types ohlcv,fundamentals,earnings

# 2-year history for backtest prep
market-data warm --watchlist watchlist.txt --days 730 --types ohlcv

# Schedule nightly (cron example)
0 18 * * 1-5 cd /path/to/mds && market-data warm --watchlist watchlist.txt --days 5 >> /var/log/mds-warm.log 2>&1
```

---

### `list-data` — Audit Local Coverage Manifest

Lists all locally available data as recorded in the coverage manifest (SQLite). Does not query TimescaleDB — reports on what ranges have been successfully ingested.

```
market-data list-data [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(all)_ | Filter by symbol |
| `--type` | `-t` | DataType | _(all)_ | Filter by data type |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "records": [
    {
      "symbol": "AAPL",
      "data_type": "ohlcv",
      "interval": "1d",
      "start_date": "2024-03-21",
      "end_date": "2025-03-21",
      "provider": "alpha_vantage",
      "row_count": 252,
      "fetched_at": "2025-03-21T18:30:00+00:00"
    }
  ],
  "count": 1
}
```

**Examples:**

```bash
# List everything
market-data list-data

# Filter by symbol
market-data list-data --symbol SPY

# Filter by data type
market-data list-data --type ohlcv

# Count total records
market-data list-data | jq '.count'

# Find all symbols that have options data
market-data list-data --type options_chain | jq '[.records[].symbol] | unique'
```

---

### `health` — Infrastructure Health Check

Checks connectivity to all components: TimescaleDB, Redis, MinIO, and each configured provider. **Exit code 0** = all healthy. **Exit code 1** = degraded. Designed for Docker health checks and monitoring.

```
market-data health [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "timescaledb": true,
  "redis": true,
  "minio": false,
  "providers": {
    "alpha_vantage": true,
    "finnhub": false
  },
  "overall": true
}
```

`overall` is `true` when TimescaleDB, Redis, and at least one provider are all healthy. MinIO failure does not affect `overall` (cold archive is optional).

**Examples:**

```bash
# Basic health check
market-data health

# Use exit code in scripts
market-data health > /dev/null && echo "healthy" || echo "DEGRADED"

# Extract specific component
market-data health | jq '.timescaledb'

# Docker health check example
HEALTHCHECK CMD market-data health > /dev/null 2>&1 || exit 1
```

---

### `audit` — Data Quality Audit

Fetches data (using the normal cache-first flow) and then runs basic quality checks: gap detection, null field counts, and price anomaly detection (daily moves >10%).

```
market-data audit --symbol SYMBOL [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(required)_ | Ticker symbol |
| `--type` | `-t` | DataType | `ohlcv` | Data type to audit |
| `--days` | `-d` | int | `365` | Days of history to audit |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "rows": 252,
  "date_range": {"start": "2024-03-21", "end": "2025-03-21"},
  "coverage": "complete",
  "gaps": [],
  "null_counts": {
    "timestamp": 0,
    "open": 0,
    "high": 0,
    "close": 0,
    "volume": 0,
    "adj_close": 3
  },
  "price_anomalies": {
    "max_daily_move_pct": 4.82,
    "days_over_10pct_move": 0
  }
}
```

**Examples:**

```bash
# Audit 1 year of AAPL OHLCV
market-data audit --symbol AAPL

# Check for data quality issues
market-data audit --symbol TSLA --days 365 | \
  jq '{gaps: (.gaps | length), anomalies: .price_anomalies.days_over_10pct_move}'

# Audit options data
market-data audit --symbol SPY --type options_chain
```

---

### `options-chain` — Query Stored Options Chain

Queries the `options_snapshots` table for an options chain. Returns all strikes with bid/ask/last, volume, open interest, IV, and Greeks (delta, gamma, theta, vega, rho) from the stored snapshot.

> **Data source:** This command reads from TimescaleDB only. It does **not** call a live provider. Options data must already be stored via `get --type options_chain` or the Databento import script (`scripts/import_databento_options.py`).

```
market-data options-chain --symbol SYMBOL [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(required)_ | Underlying ticker, e.g. `SPY` |
| `--expiration` | `-e` | YYYY-MM-DD | _(all expirations)_ | Filter to a specific expiration date |
| `--type` | `-t` | `call`\|`put` | _(both)_ | Filter by option type |
| `--snapshot-date` | | YYYY-MM-DD | _(latest)_ | Use data from a specific historical snapshot |
| `--format` | | `json`\|`csv` | `json` | Output format |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "symbol": "SPY",
  "expiration": "2024-06-21",
  "option_type": "call",
  "rows": 78,
  "schema": [
    "snapshot_at", "symbol", "expiration_date", "strike", "option_type",
    "bid", "ask", "last", "volume", "open_interest", "implied_volatility",
    "delta", "gamma", "theta", "vega", "rho", "iv_rank", "iv_percentile",
    "underlying_price", "provider"
  ],
  "data": [
    {
      "snapshot_at": "2024-06-01T00:00:00+00:00",
      "symbol": "SPY",
      "expiration_date": "2024-06-21",
      "strike": 530.0,
      "option_type": "call",
      "bid": 2.15,
      "ask": 2.20,
      "last": 2.17,
      "volume": 8432,
      "open_interest": 42150,
      "implied_volatility": 0.182,
      "delta": 0.41,
      "gamma": 0.038,
      "theta": -0.12,
      "vega": 0.28,
      "rho": 0.009,
      "iv_rank": 62.0,
      "iv_percentile": 67.0,
      "underlying_price": 528.40,
      "provider": "finnhub"
    }
  ]
}
```

**Field reference:**

| Field | Description |
|-------|-------------|
| `snapshot_at` | UTC timestamp when the snapshot was taken |
| `expiration_date` | Option expiration date |
| `strike` | Strike price |
| `option_type` | `call` or `put` |
| `bid` / `ask` / `last` | Bid, ask, and last trade prices |
| `volume` | Day's trading volume |
| `open_interest` | Open contracts |
| `implied_volatility` | IV as a decimal (0.182 = 18.2%) |
| `delta` | Rate of change vs. underlying (−1 to +1) |
| `gamma` | Rate of change of delta |
| `theta` | Daily time decay (negative for long options) |
| `vega` | Sensitivity to 1% IV move |
| `rho` | Sensitivity to interest rate change |
| `iv_rank` | 0–100: where current IV sits vs. its 52-week range |
| `iv_percentile` | 0–100: % of days with lower IV in lookback |
| `underlying_price` | Spot price at snapshot time |
| `provider` | Data source (`finnhub`, `databento`, etc.) |

> **Note on Databento OHLCV-1D data:** When options are imported from the Databento historical dataset (`scripts/import_databento_options.py`), `bid`, `ask`, `open_interest`, `implied_volatility`, and all Greeks will be `null`. Only `last` (= EOD close) and `volume` are populated.

**Examples:**

```bash
# Full chain, latest snapshot, all expirations
market-data options-chain --symbol SPY

# Specific expiration (all strikes, both calls and puts)
market-data options-chain --symbol SPY --expiration 2024-06-21

# Calls only for a specific expiration
market-data options-chain --symbol AAPL --expiration 2024-06-21 --type call

# Historical snapshot (backtesting)
market-data options-chain --symbol SPY --snapshot-date 2024-01-15

# Specific expiration from a historical snapshot
market-data options-chain --symbol SPY \
  --expiration 2024-01-19 \
  --snapshot-date 2024-01-15

# CSV output (useful for spreadsheet import)
market-data options-chain --symbol SPY --expiration 2024-06-21 --format csv

# Agent: get all ATM strikes (within 1% of spot)
market-data options-chain --symbol SPY --expiration 2024-06-21 | \
  jq --argjson spot 528 \
  '[.data[] | select((.strike >= ($spot * 0.99)) and (.strike <= ($spot * 1.01)))]'

# Agent: get high-OI puts for a spread
market-data options-chain --symbol SPY --expiration 2024-06-21 --type put | \
  jq '[.data[] | select(.open_interest > 10000)] | sort_by(-.open_interest)'

# Agent: build a delta-filtered call list
market-data options-chain --symbol SPY --expiration 2024-06-21 --type call | \
  jq '[.data[] | select(.delta != null and .delta >= 0.25 and .delta <= 0.40)]'
```

**Agent usage pattern (building a spread):**

```python
import subprocess, json

def get_options_chain(symbol: str, expiration: str, option_type: str) -> list[dict]:
    result = subprocess.run(
        ["market-data", "options-chain",
         "--symbol", symbol,
         "--expiration", expiration,
         "--type", option_type],
        capture_output=True, text=True, check=True
    )
    response = json.loads(result.stdout)
    if response["rows"] == 0:
        raise ValueError(f"No options data for {symbol} exp {expiration}")
    return response["data"]

calls = get_options_chain("SPY", "2024-06-21", "call")
puts  = get_options_chain("SPY", "2024-06-21", "put")

# Find the 30-delta call and 30-delta put for a strangle
short_call = min(calls, key=lambda x: abs((x["delta"] or 0) - 0.30))
short_put  = min(puts,  key=lambda x: abs((x["delta"] or 0) + 0.30))
```

---

### `iv-rank` — Query IV Rank History

Queries the `iv_rank_history` table for a symbol's historical IV rank and IV percentile series.

> **Data source:** Reads from TimescaleDB only. IV rank must be pre-computed and stored via the `upsert_iv_rank()` storage method. A future provider implementation (Finnhub or Databento) will populate this automatically via `get --type iv_rank`.

```
market-data iv-rank --symbol SYMBOL [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(required)_ | Ticker symbol |
| `--days` | `-d` | int | `252` | Lookback window (252 ≈ 1 trading year, 504 ≈ 2 years) |
| `--format` | | `json`\|`csv` | `json` | Output format |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "symbol": "SPY",
  "lookback_days": 252,
  "current_iv_rank": 68.4,
  "current_iv_percentile": 72.1,
  "current_iv": 0.192,
  "rows": 252,
  "schema": [
    "recorded_at", "symbol", "iv_rank", "iv_percentile",
    "current_iv", "iv_52w_high", "iv_52w_low", "provider"
  ],
  "data": [
    {
      "recorded_at": "2025-03-21",
      "symbol": "SPY",
      "iv_rank": 68.4,
      "iv_percentile": 72.1,
      "current_iv": 0.192,
      "iv_52w_high": 0.381,
      "iv_52w_low": 0.108,
      "provider": "finnhub"
    }
  ]
}
```

**Field reference:**

| Field | Description |
|-------|-------------|
| `iv_rank` | `(current_iv − iv_52w_low) / (iv_52w_high − iv_52w_low) × 100` |
| `iv_percentile` | % of trading days in lookback where IV was lower than today |
| `current_iv` | ATM implied volatility as decimal (0.192 = 19.2%) |
| `iv_52w_high` | Highest IV in the 52-week window |
| `iv_52w_low` | Lowest IV in the 52-week window |

**IV rank interpretation:**

| Range | IV Condition | Strategy Signal |
|-------|-------------|-----------------|
| 0–20 | Depressed IV | Favor long premium (buy straddles/strangles) |
| 20–50 | Normal IV | Neutral; use directional strategies |
| 50–80 | Elevated IV | Favor short premium (sell credit spreads) |
| 80–100 | Extreme IV | Aggressive short premium; expect reversion |

**Examples:**

```bash
# Current IV rank (252-day default)
market-data iv-rank --symbol SPY

# 2-year lookback
market-data iv-rank --symbol AAPL --days 504

# Check current rank only (no history)
market-data iv-rank --symbol SPY | jq '{iv_rank: .current_iv_rank, iv_percentile: .current_iv_percentile}'

# Plot-ready CSV
market-data iv-rank --symbol SPY --days 365 --format csv > spy_iv_rank.csv

# Agent: decide strategy direction
market-data iv-rank --symbol SPY | jq 'if .current_iv_rank > 50 then "SELL_PREMIUM" else "BUY_PREMIUM" end'
```

---

### `max-pain` — Compute Max Pain Strike

Calculates the max pain price for a specific options expiration using stored open interest data.

**Max pain theory:** The strike where the total monetary loss to all option *holders* (both calls and puts) is maximized — equivalently, where option *writers* profit most. Used as a gravitational pull estimate for where the underlying may pin near expiration.

**Formula:**
For each candidate strike *S*, compute:
`pain(S) = Σ_K [ call_OI(K) × max(0, S−K) + put_OI(K) × max(0, K−S) ]`
Max pain = the strike *S* that minimizes `pain(S)`.

> **Requirement:** Open interest data must be present. Databento OHLCV-1D imports do not include OI (`open_interest` is `null`). OI-populated data must come from Finnhub or another live options provider.

```
market-data max-pain --symbol SYMBOL --expiration DATE [OPTIONS]
```

**Options:**

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | string | _(required)_ | Underlying ticker |
| `--expiration` | `-e` | YYYY-MM-DD | _(required)_ | Option expiration date |
| `--snapshot-date` | | YYYY-MM-DD | _(latest)_ | Use OI from a specific historical snapshot |
| `--verbose` | `-v` | flag | false | Debug logging |

**Output (JSON):**

```json
{
  "symbol": "SPY",
  "expiration": "2024-06-21",
  "max_pain_price": 528.0,
  "snapshot_date": "2024-06-01",
  "strikes": [500.0, 505.0, 510.0, 515.0, 520.0, 525.0, 528.0, 530.0, 535.0],
  "call_oi": [12000, 8500, 9200, 11000, 14500, 18000, 8000, 22000, 6500],
  "put_oi":  [22000, 18500, 15000, 12000, 10500, 9000, 4000, 3500, 2000],
  "total_pain": [4200000, 3800000, 3400000, 3050000, 2800000, 2600000, 2550000, 2700000, 3100000]
}
```

**Examples:**

```bash
# Max pain for upcoming monthly expiration
market-data max-pain --symbol SPY --expiration 2024-06-21

# Max pain using historical snapshot (backtesting)
market-data max-pain --symbol SPY \
  --expiration 2024-06-21 \
  --snapshot-date 2024-06-14

# Extract just the price
market-data max-pain --symbol SPY --expiration 2024-06-21 | jq '.max_pain_price'

# Agent: compare max pain to current spot for pin risk
MAX_PAIN=$(market-data max-pain --symbol SPY --expiration 2024-06-21 | jq '.max_pain_price')
echo "Max pain is at $MAX_PAIN"

# Plot the pain curve (Python)
market-data max-pain --symbol SPY --expiration 2024-06-21 | python3 -c "
import json, sys
d = json.load(sys.stdin)
for s, p in zip(d['strikes'], d['total_pain']):
    print(f'{s:.0f}: {p:,.0f}')
"
```

---

## Common Workflows

### Workflow 1: End-of-Day Data Refresh

```bash
# Warm the full watchlist nightly
market-data warm --watchlist watchlist.txt --days 5 --types ohlcv,fundamentals

# Verify no gaps
market-data status --symbol SPY --days 5 | jq '.coverage'
```

### Workflow 2: Options Strategy Preparation

```bash
# Step 1: Check what expirations are available
market-data list-data --symbol SPY --type options_chain

# Step 2: Get the full chain for target expiration
market-data options-chain --symbol SPY --expiration 2024-06-21

# Step 3: Check IV environment
market-data iv-rank --symbol SPY | jq '{rank: .current_iv_rank, percentile: .current_iv_percentile}'

# Step 4: Check where max pain sits
market-data max-pain --symbol SPY --expiration 2024-06-21 | jq '.max_pain_price'
```

### Workflow 3: Backtesting Data Validation

```bash
# Check data completeness for backtest period
market-data status --symbol SPY --start 2024-01-01 --end 2024-12-31

# Audit data quality
market-data audit --symbol SPY --days 365 | \
  jq '{gaps: (.gaps | length), max_move: .price_anomalies.max_daily_move_pct}'

# Pull options chain as of specific historical date
market-data options-chain --symbol SPY \
  --expiration 2024-01-19 \
  --snapshot-date 2024-01-12 \
  --format csv > spy_chain_20240112.csv
```

### Workflow 4: Multi-Symbol Fundamentals Sweep

```bash
# Batch fundamentals for a portfolio
market-data batch --symbols AAPL,MSFT,GOOGL,AMZN,META --type fundamentals | \
  jq '.results | to_entries[] | {symbol: .key, pe: (.value.data[0].pe_ratio // "N/A")}'
```

### Workflow 5: First-Time Databento Historical Import

```bash
# 1. Ensure TimescaleDB is running
market-data health | jq '.timescaledb'

# 2. Run the import (2.3M rows, ~4 minutes)
MDS_TIMESCALE_URL=postgresql://mds:mds_secret@localhost:5433/market_data \
  .venv/bin/python scripts/import_databento_options.py

# 3. Verify data is present
market-data list-data --type options_chain

# 4. Spot check a date
market-data options-chain --symbol SPY \
  --snapshot-date 2024-03-08 \
  --expiration 2024-03-15 | jq '.rows'
```

---

## Output Formats

### JSON (default)

All commands output a structured JSON object to stdout. The exact shape varies by command (see each command's **Output** section above). Common conventions:

- `rows` — always present; the count of data records in `data`
- `schema` — column names in the order they appear in `data` rows
- `data` — array of row objects
- Dates and timestamps are ISO 8601 strings
- `null` fields are included (not omitted) so schema is consistent
- `null` numeric fields indicate data not available (not zero)

### CSV

Pass `--format csv` to `get`, `options-chain`, and `iv-rank`. Output goes to stdout with a header row. Useful for piping to files or spreadsheet tools.

```bash
market-data get --symbol AAPL --days 365 --format csv > aapl_ohlcv.csv
market-data options-chain --symbol SPY --expiration 2024-06-21 --format csv > spy_chain.csv
market-data iv-rank --symbol SPY --days 252 --format csv > spy_iv_rank.csv
```

---

## Exit Codes

| Code | Meaning |
|------|---------|
| `0` | Success |
| `1` | Error (command failed, infra unreachable, or `health` reported degraded) |

---

## Error Handling

Errors are printed to **stderr** using Rich formatting. Stdout remains clean JSON even on partial failures.

```bash
# Capture errors separately
market-data get --symbol AAPL 2>errors.log | jq '.coverage'

# Verbose mode for debugging
market-data get --symbol AAPL --type ohlcv --verbose 2>&1 | head -30
```

**Common error patterns and remedies:**

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `No options data stored for SPY` | Options not yet loaded | Run `import_databento_options.py` or `get --type options_chain` |
| `No IV rank history for AAPL` | IV rank table empty | Populate via `upsert_iv_rank()` or future provider integration |
| `connection refused` on port 5433 | TimescaleDB not running | `docker compose up -d timescaledb` |
| `Authentication required` for Redis | Wrong Redis or password | Check `REDIS_URL` in `.env` |
| `Alpha Vantage API limit` | Rate limit hit | Wait 1 minute; MDS retries automatically up to `MDS_MAX_RETRIES` |
| `Provider not found for data_type` | No provider supports the type | Check provider configuration and API keys |

---

## Quick Reference Card

```bash
# ── Core Data ────────────────────────────────────────────────────────────────
market-data get -s AAPL                                    # 365d daily OHLCV
market-data get -s AAPL -t ohlcv --start 2024-01-01       # explicit start
market-data get -s AAPL -t ohlcv -d 30 -f                 # 30d, force refresh
market-data get -s TSLA -t ohlcv_intraday -i 15m -d 30    # 15min intraday
market-data get -s AAPL -t fundamentals                    # latest fundamentals
market-data get -s AAPL -t earnings                        # earnings history
market-data get -s AAPL -t news_sentiment -d 30            # recent news
market-data get -s AAPL -t dividends                       # dividend history

# ── Coverage & Audit ─────────────────────────────────────────────────────────
market-data status -s AAPL                                 # check coverage
market-data list-data                                      # all cached data
market-data list-data -s SPY                               # per symbol
market-data audit -s AAPL                                  # quality check

# ── Multi-Symbol ─────────────────────────────────────────────────────────────
market-data batch --symbols AAPL,MSFT,SPY                  # parallel fetch
market-data warm --watchlist watchlist.txt                 # warm from file
market-data warm --watchlist AAPL,SPY --types ohlcv        # warm inline

# ── Options ──────────────────────────────────────────────────────────────────
market-data options-chain -s SPY                           # full chain, latest
market-data options-chain -s SPY -e 2024-06-21             # specific expiry
market-data options-chain -s SPY -e 2024-06-21 -t call     # calls only
market-data options-chain -s SPY --snapshot-date 2024-01-15  # historical
market-data iv-rank -s SPY                                 # current IV rank
market-data iv-rank -s SPY -d 504                          # 2-year lookback
market-data max-pain -s SPY -e 2024-06-21                  # max pain strike

# ── Infrastructure ───────────────────────────────────────────────────────────
market-data health                                         # component status
docker compose up -d timescaledb redis                     # start infra
docker compose down timescaledb redis                      # stop infra
```
