# Market Data Service — CLI Command Outcomes

This document lists every command, subcommand, and option in the `market-data` CLI with its
expected outcome. Use the **Validation** section of each command to confirm the behaviour
described matches what the running service produces.

All commands write **JSON to stdout** (machine-readable) and human-readable progress/errors
to **stderr** (Rich-formatted, does not pollute stdout).

---

## Global Flags

Available on every command.

| Flag | Short | Type | Default | Effect |
|------|-------|------|---------|--------|
| `--verbose` | `-v` | flag | false | Enables DEBUG-level logging to stderr; prints Python traceback on exception |
| `--help` | | flag | | Prints command help and exits |

---

## Commands

### 1. `get` — Fetch Market Data

**Purpose:** Primary data access. Cache-first, delta-fetch. Checks Redis hot cache, then
TimescaleDB. Only missing date ranges are fetched from upstream providers. The fetched gaps
are written back to TimescaleDB, Redis, and the coverage manifest before returning.

#### Full Command Syntax

```
market-data get --symbol SYMBOL
                [--type ohlcv|ohlcv_intraday|options_chain|fundamentals|
                        news_sentiment|earnings|dividends|iv_rank|tick|futures_ohlcv]
                [--start YYYY-MM-DD]
                [--end YYYY-MM-DD]
                [--days N]
                [--interval 1m|5m|15m|1h|4h|1d|1w]
                [--force-refresh]
                [--provider PROVIDER_NAME]
                [--format json|csv]
                [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol (e.g. `AAPL`, `SPY`) |
| `--type` | `-t` | DataType | `ohlcv` | Data type to fetch — see table below |
| `--start` | | YYYY-MM-DD | `today - days` | Start date; overrides `--days` |
| `--end` | | YYYY-MM-DD | today | End date |
| `--days` | `-d` | int | 365 | Calendar days back from today (used when `--start` omitted) |
| `--interval` | `-i` | Interval | `1d` | Bar interval — relevant for `ohlcv` and `ohlcv_intraday` |
| `--force-refresh` | `-f` | flag | false | Bypass Redis and TimescaleDB; always fetch from provider |
| `--provider` | `-p` | str | auto | Override provider selection (e.g. `alpha_vantage`, `finnhub`) |
| `--format` | | `json`\|`csv` | `json` | Output format written to stdout |
| `--verbose` | `-v` | flag | false | DEBUG logging + traceback on error |

#### `--type` Values

| Value | Description | Default Provider Chain |
|-------|-------------|------------------------|
| `ohlcv` | End-of-day OHLCV bars | yfinance → alpha_vantage → finnhub → databento |
| `ohlcv_intraday` | Intraday bars (must pair with `--interval`) | databento → finnhub → alpha_vantage |
| `options_chain` | Full options chain snapshot (strikes, OI, Greeks, IV) | tastytrade → databento → finnhub |
| `fundamentals` | P/E, EPS, revenue, market cap, sector, industry | alpha_vantage → finnhub |
| `news_sentiment` | Headlines with sentiment score −1.0 to 1.0 | finnhub → alpha_vantage |
| `earnings` | Earnings history with EPS actual vs. estimate | alpha_vantage → finnhub |
| `dividends` | Ex-date, amount, pay date | alpha_vantage |
| `iv_rank` | IV rank history (separate table, not options chain) | databento → finnhub |
| `tick` | Raw tick data | databento |
| `futures_ohlcv` | Futures OHLCV (ES, NQ, etc.) | databento |

#### `--interval` Values

| Value | Description |
|-------|-------------|
| `1m` | 1-minute bars |
| `5m` | 5-minute bars |
| `15m` | 15-minute bars |
| `1h` | 1-hour bars |
| `4h` | 4-hour bars |
| `1d` | Daily bars (default) |
| `1w` | Weekly bars |

#### Expected Outcome

**Exit code:** `0` on success (including partial coverage), `1` on exception.

**stdout (JSON, default format):**

```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "interval": "1d",
  "source": "timescaledb",
  "coverage": "complete",
  "gaps": [],
  "rows": 252,
  "fetched_at": "2026-04-25T14:00:00+00:00",
  "schema": ["timestamp", "symbol", "open", "high", "low", "close", "volume", "adj_close", "provider"],
  "data": [
    {
      "timestamp": "2025-04-25T00:00:00+00:00",
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

**`source` values:**
- `cache` — served from Redis hot cache (sub-millisecond)
- `timescaledb` — full cache hit from local DB; no API call made
- `merged` — partial cache hit; gaps fetched from provider and merged
- `api:alpha_vantage`, `api:finnhub`, `api:databento`, `api:tastytrade` — entire range fetched from that provider

**`coverage` values:**
- `complete` — all requested dates present locally
- `partial` — some dates present; gaps listed in `gaps` array
- `missing` — no local data; all rows fetched from provider

**stdout (CSV format — when `--format csv`):**
```
timestamp,symbol,open,high,low,close,volume,adj_close,provider
2025-04-25T00:00:00+00:00,AAPL,215.50,218.90,214.20,217.30,62450000,217.30,alpha_vantage
...
```

**stderr (human-readable):** Progress messages from Rich; errors in red.

#### Validation Examples

```bash
# Fetch 365 days of AAPL daily OHLCV (default type and interval)
market-data get --symbol AAPL

# Expected: JSON with data_type="ohlcv", interval="1d", rows ~252 (trading days)

# Fetch 2 days of SPY 5-minute intraday bars
market-data get --symbol SPY --type ohlcv_intraday --interval 5m --days 2

# Expected: JSON with data_type="ohlcv_intraday", interval="5m", ~156 rows (2 days × 78 bars/day)

# Fetch AAPL fundamentals via a specific provider
market-data get --symbol AAPL --type fundamentals --provider alpha_vantage

# Expected: JSON with data_type="fundamentals", fields include pe_ratio, eps, revenue, market_cap, sector

# Fetch SPY options chain snapshot for a specific date
market-data get --symbol SPY --type options_chain --provider databento --start 2026-03-27 --end 2026-03-27

# Expected: JSON with data_type="options_chain", schema includes strike, option_type, bid, ask, delta, etc.

# Force-refresh (bypasses all caches)
market-data get --symbol AAPL --force-refresh

# Expected: source = "api:<provider_name>", fresh data regardless of what is cached

# CSV output for pipeline use
market-data get --symbol SPY --days 90 --format csv | head -5

# Expected: CSV header row followed by OHLCV data rows
```

---

### 2. `status` — Check Local Coverage

**Purpose:** Reports what data is stored locally for a symbol/type range. **Read-only — never
fetches from providers, never modifies state.** Use this before `get` to check if data is
already available.

#### Full Command Syntax

```
market-data status --symbol SYMBOL
                   [--type DATA_TYPE]
                   [--start YYYY-MM-DD]
                   [--end YYYY-MM-DD]
                   [--days N]
                   [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol |
| `--type` | `-t` | DataType | `ohlcv` | Data type to check |
| `--start` | | YYYY-MM-DD | `today - days` | Start of range to check |
| `--end` | | YYYY-MM-DD | today | End of range to check |
| `--days` | `-d` | int | 365 | Days back from today (used when `--start` omitted) |
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Expected Outcome

**Exit code:** `0` on success, `1` on exception.

**stdout (JSON):**

```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "coverage": "partial",
  "available_ranges": [
    {"start": "2025-01-01", "end": "2025-12-31"}
  ],
  "gaps": [
    {"start": "2024-04-25", "end": "2024-12-31"}
  ],
  "total_rows": null
}
```

**`coverage` values:** `complete`, `partial`, `missing` (same as `get`).

**`available_ranges`:** Array of `{"start": "...", "end": "..."}` objects showing stored date segments.

**`gaps`:** Date ranges within the requested window that have no local data.

**`total_rows`:** Row count from coverage manifest when available; may be `null`.

#### Validation Examples

```bash
# Check 365 days of AAPL OHLCV coverage
market-data status --symbol AAPL

# Expected: coverage = "complete" if previously fetched, "missing" if not

# Check a specific date range
market-data status --symbol SPY --start 2024-01-01 --end 2024-12-31

# Expected: available_ranges shows stored segments; gaps shows missing ranges

# Check fundamentals coverage
market-data status --symbol TSLA --type fundamentals --days 90

# Expected: coverage indicates whether fundamentals data exists locally
```

---

### 3. `batch` — Parallel Fetch for Multiple Symbols

**Purpose:** Fetch data for multiple symbols in parallel using a thread pool. Returns a
combined response with per-symbol results plus `succeeded`/`failed` lists.

#### Full Command Syntax

```
market-data batch --symbols SYM1,SYM2,...
                  [--type DATA_TYPE]
                  [--days N]
                  [--interval INTERVAL]
                  [--workers N]
                  [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbols` | | str | **required** | Comma-separated symbols, e.g. `AAPL,TSLA,SPY` |
| `--type` | `-t` | DataType | `ohlcv` | Data type |
| `--days` | `-d` | int | 365 | Days back from today |
| `--interval` | | Interval | `1d` | Bar interval |
| `--workers` | `-w` | int | 4 | Thread-pool size for parallel fetching |
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Expected Outcome

**Exit code:** `0` even when some symbols fail (partial success), `1` on fatal exception.

**stdout (JSON):**

```json
{
  "requested": ["AAPL", "TSLA", "SPY"],
  "succeeded": ["AAPL", "SPY"],
  "failed": ["TSLA"],
  "results": {
    "AAPL": {
      "symbol": "AAPL",
      "data_type": "ohlcv",
      "interval": "1d",
      "source": "timescaledb",
      "coverage": "complete",
      "gaps": [],
      "rows": 252,
      "fetched_at": "2026-04-25T14:00:00+00:00",
      "schema": ["timestamp", "symbol", "open", "high", "low", "close", "volume", "adj_close", "provider"],
      "data": [...]
    },
    "SPY": { "...": "..." }
  }
}
```

- `requested` — symbols passed in (uppercased, whitespace stripped)
- `succeeded` — symbols with successful DataResponse
- `failed` — symbols that raised exceptions (provider error, timeout, etc.)
- `results` — dict of `symbol → DataResponse`; only includes succeeded symbols

#### Validation Examples

```bash
# Fetch 90 days of OHLCV for 3 symbols using 8 workers
market-data batch --symbols AAPL,TSLA,SPY --type ohlcv --days 90 --workers 8

# Expected: JSON with all 3 in "requested"; check "failed" is empty if providers are healthy

# Check how many failed. This only shows a number if it cannot get the data from the provider. If the data is not in the cache, it will fetch it from the provider. Therefore, the result should always be 0 if the providers are healthy.
market-data batch --symbols AAPL,MSFT,GOOG,AMZN --days 30 | jq '.failed | length'

# Expected: 0 if all succeeded, >0 if provider errors occurred
```

---

### 4. `warm` — Pre-populate Cache for a Watchlist

**Purpose:** Pre-fetch and store data for a list of symbols before trading sessions or
strategy runs. Accepts either a file path (one symbol per line, `#` comments ignored) or
an inline comma-separated list.

#### Full Command Syntax

```
market-data warm --watchlist FILE_OR_SYMBOLS
                 [--days N]
                 [--types DATA_TYPE1,DATA_TYPE2,...]
                 [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--watchlist` | `-w` | str | **required** | File path OR comma-separated symbols |
| `--days` | `-d` | int | 365 | Days back from today |
| `--types` | | str | `ohlcv,fundamentals` | Comma-separated data types to fetch |
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Watchlist File Format

```
# My Trading Symbols
AAPL
MSFT
SPY
# Energy sector
XLE
CVX
```

Lines starting with `#` and blank lines are ignored. Symbols are uppercased automatically.

#### Expected Outcome

**Exit code:** `0` (partial success allowed), `1` on fatal exception.

**stderr:** Progress message before fetching: `Warming N symbols × M data types × D days...`

**stdout (JSON):**

```json
{
  "symbols": ["AAPL", "MSFT", "SPY"],
  "data_types": ["ohlcv", "fundamentals"],
  "results": {
    "ohlcv": {
      "succeeded": ["AAPL", "MSFT", "SPY"],
      "failed": []
    },
    "fundamentals": {
      "succeeded": ["AAPL", "MSFT", "SPY"],
      "failed": []
    }
  }
}
```

#### Validation Examples

```bash
# Warm OHLCV only using inline symbol list
market-data warm --watchlist AAPL,MSFT,SPY --types ohlcv --days 365

# Expected: JSON showing all 3 symbols in ohlcv.succeeded

# Warm from a file with multiple types
market-data warm --watchlist watchlist.txt --days 365 --types ohlcv,fundamentals

# Expected: results object with a key per data type; each lists succeeded/failed symbols

# Verify after warming
market-data status --symbol AAPL
# Expected: coverage = "complete" for the warmed date range
```

---

### 5. `list-data` — List Locally Available Data

**Purpose:** Audit what data is stored in the local coverage manifest without making any
API calls. Optionally filter by symbol and/or data type.

#### Full Command Syntax

```
market-data list-data [--symbol SYMBOL]
                      [--type DATA_TYPE]
                      [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | (none) | Filter by symbol (optional) |
| `--type` | `-t` | DataType | (none) | Filter by data type (optional) |
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Expected Outcome

**Exit code:** `0`, `1` on exception.

**stdout (JSON):**

```json
{
  "records": [
    {
      "symbol": "AAPL",
      "data_type": "ohlcv",
      "interval": "1d",
      "start_date": "2025-04-25",
      "end_date": "2026-04-25",
      "provider": "alpha_vantage",
      "row_count": 252,
      "fetched_at": "2026-04-25T10:00:00"
    },
    {
      "symbol": "SPY",
      "data_type": "options_chain",
      "interval": null,
      "start_date": "2026-03-27",
      "end_date": "2026-03-27",
      "provider": "databento",
      "row_count": 13496,
      "fetched_at": "2026-03-28T09:00:00"
    }
  ],
  "count": 2
}
```

When no records match: `{"records": [], "count": 0}`

#### Validation Examples

```bash
# List all locally cached data
market-data list-data

# Expected: records array with one entry per symbol/type/interval combination stored

# Filter to a specific symbol
market-data list-data --symbol SPY

# Expected: only SPY records

# Filter to a specific data type
market-data list-data --type options_chain

# Expected: only options_chain records across all symbols

# Check count
market-data list-data | jq '.count'
# Expected: integer ≥ 0
```

---

### 6. `health` — Infrastructure Health Check

**Purpose:** Check connectivity to all infrastructure components (TimescaleDB, Redis, MinIO)
and each configured data provider. Returns a `true`/`false` for each component.

#### Full Command Syntax

```
market-data health [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Expected Outcome

**Exit code:** `0` when `overall` is `true`, `1` when `overall` is `false`.

**stderr:** `Checking infrastructure health...` before any checks run.

**stdout (JSON):**

```json
{
  "timescaledb": true,
  "redis": true,
  "minio": true,
  "providers": {
    "alpha_vantage": true,
    "finnhub": false,
    "databento": true,
    "tastytrade": true,
    "yfinance": true
  },
  "overall": true
}
```

- `overall` is `true` only when all components are healthy
- A missing or unconfigured provider (e.g. no API key) appears as `false`

#### Validation Examples

```bash
# Run health check
market-data health

# Expected: JSON with boolean values for each component; exit code 0 if overall=true

# Use in a shell script
market-data health && echo "MDS healthy" || echo "MDS degraded"

# Inspect individual provider status
market-data health | jq '.providers'

# Expected: object with provider names as keys and true/false values
```

---

### 7. `audit` — Data Quality Audit

**Purpose:** Analyze data quality for a stored symbol: checks coverage status, identifies
null fields, and detects abnormal price moves that may indicate bad data. Calls `get`
internally then runs statistical checks on the result.

#### Full Command Syntax

```
market-data audit --symbol SYMBOL
                  [--type DATA_TYPE]
                  [--days N]
                  [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol |
| `--type` | `-t` | DataType | `ohlcv` | Data type to audit |
| `--days` | `-d` | int | 365 | Days back from today |
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Expected Outcome

**Exit code:** `0` whether data is clean or not (audit completed successfully), `1` on exception.

**stdout (JSON):**

```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "rows": 252,
  "date_range": {
    "start": "2025-04-25",
    "end": "2026-04-25"
  },
  "coverage": "complete",
  "gaps": [],
  "null_counts": {
    "timestamp": 0,
    "open": 0,
    "high": 0,
    "low": 0,
    "close": 0,
    "volume": 0,
    "adj_close": 12
  },
  "price_anomalies": {
    "max_daily_move_pct": 8.5,
    "days_over_10pct_move": 3
  }
}
```

- `null_counts` — per-column count of null/NaN values; zero is ideal
- `price_anomalies.max_daily_move_pct` — largest single-day close-to-close % move
- `price_anomalies.days_over_10pct_move` — number of days with a move >10% (potential data errors)
- `price_anomalies` key is **absent** when `close` column is not in the schema (e.g. fundamentals)

#### Validation Examples

```bash
# Audit 365 days of AAPL OHLCV
market-data audit --symbol AAPL

# Expected: rows ~252, coverage="complete", null_counts all zeros for core OHLCV fields

# Check for any nulls in adj_close
market-data audit --symbol AAPL | jq '.null_counts.adj_close'

# Expected: 0 if data is clean, >0 if some adj_close values are missing

# Audit fundamentals
market-data audit --symbol TSLA --type fundamentals

# Expected: rows ~1, no price_anomalies key (no "close" column in fundamentals schema)

# Check for suspicious price moves
market-data audit --symbol AAPL | jq '.price_anomalies.days_over_10pct_move'
# Expected: small integer; >5 may warrant investigation
```

---

### 8. `options-chain` — Query Stored Options Chain

**Purpose:** Query stored options chain snapshots from the local TimescaleDB. Returns all
strikes for a symbol, optionally filtered by expiration date, option type, and snapshot date.
Reads only — never fetches from providers.

#### Full Command Syntax

```
market-data options-chain --symbol SYMBOL
                          [--expiration YYYY-MM-DD]
                          [--type call|put]
                          [--snapshot-date YYYY-MM-DD]
                          [--format json|csv]
                          [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol (e.g. `SPY`) |
| `--expiration` | `-e` | YYYY-MM-DD | latest available | Filter to a specific expiration |
| `--type` | `-t` | `call`\|`put` | both | Filter by option type |
| `--snapshot-date` | | YYYY-MM-DD | latest available | Which snapshot date to query |
| `--format` | | `json`\|`csv` | `json` | Output format |
| `--verbose` | `-v` | flag | false | DEBUG logging + traceback on error |

#### Expected Outcome

**Exit code:** `0` on success (including when no data found), `1` on exception.

**stdout (JSON, data found):**

```json
{
  "symbol": "SPY",
  "expiration": "2026-03-20",
  "option_type": "call",
  "rows": 47,
  "schema": [
    "snapshot_at", "symbol", "expiration_date", "strike", "option_type",
    "bid", "ask", "last", "volume", "open_interest", "implied_volatility",
    "delta", "gamma", "theta", "vega", "rho",
    "iv_rank", "iv_percentile", "underlying_price", "provider"
  ],
  "data": [
    {
      "snapshot_at": "2026-03-20T16:00:00",
      "symbol": "SPY",
      "expiration_date": "2026-03-20",
      "strike": 420.0,
      "option_type": "call",
      "bid": 2.45,
      "ask": 2.50,
      "last": 2.48,
      "volume": 1234,
      "open_interest": 45678,
      "implied_volatility": 0.185,
      "delta": 0.52,
      "gamma": 0.025,
      "theta": -0.08,
      "vega": 0.12,
      "rho": 0.18,
      "iv_rank": null,
      "iv_percentile": null,
      "underlying_price": 420.50,
      "provider": "databento"
    }
  ]
}
```

**stdout (JSON, no data found):** `{"symbol": "SPY", "rows": 0, "data": []}`

**stderr warning when no data:** `No options data stored for SPY. Run: market-data get --symbol SPY --type options_chain`

**Greeks availability by provider:**

| Field | TastyTrade | Databento OPRA | Finnhub |
|-------|:----------:|:--------------:|:-------:|
| `bid`/`ask` | ✓ | — | ✓ |
| `last` | ✓ | ✓ | ✓ |
| `volume` | ✓ | ✓ | ✓ |
| `open_interest` | ✓ | ✓ | ✓ |
| `implied_volatility` | ✓ | — | ✓ |
| `delta`, `gamma`, `theta`, `vega`, `rho` | ✓ | — | ✓ |

Fields not provided by a source will be `null`.

#### Validation Examples

```bash
# Get the full latest options chain for SPY
market-data options-chain --symbol SPY

# Expected: JSON with all expirations/strikes from the latest stored snapshot

# Filter to calls only for a specific expiration
market-data options-chain --symbol SPY --expiration 2026-03-28 --type call

# Expected: rows = number of call strikes for that expiration

# Query a historical snapshot
market-data options-chain --symbol SPY --snapshot-date 2026-03-27

# Expected: data from the snapshot recorded on 2026-03-27

# CSV output for spreadsheet import
market-data options-chain --symbol SPY --expiration 2026-03-28 --format csv > spy_chain.csv

# Count strikes in the chain
market-data options-chain --symbol SPY | jq '.rows'
```

---

### 9. `iv-rank` — Historical IV Rank

**Purpose:** Return historical implied volatility rank and percentile for a symbol.
If today's IV has not been stored yet, the service fetches the current ATM IV from
Databento (falling back to Finnhub, then the stored options chain), computes rank
and percentile against the lookback history, upserts the row, then returns the full history.

> **Prerequisite:** `iv-rank` requires historical data to produce a meaningful rank.
> On first use, `rows: 1` and `iv_rank: 50.0` indicate no history exists yet.
> Run `iv-rank-backfill` first (see workflow below).

#### Full Command Syntax

```
market-data iv-rank --symbol SYMBOL
                    [--days N]
                    [--force-refresh]
                    [--format json|csv]
                    [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol (e.g. `AAPL`) |
| `--days` | `-d` | int | 252 | Lookback window in calendar days (252 ≈ 1 trading year) |
| `--force-refresh` | `-f` | flag | false | Recompute today's rank/percentile from the full lookback history |
| `--format` | | `json`\|`csv` | `json` | Output format |
| `--verbose` | `-v` | flag | false | DEBUG logging + traceback on error |

#### How `--days` and `--force-refresh` interact

Today's IV rank row is computed **once** and stored. Subsequent calls within the same day
serve the stored row without recomputing, even if `--days` changes. This avoids redundant
API calls on a live trading day.

`--force-refresh` overrides this: it deletes today's stored row and recomputes rank and
percentile from scratch against the full `--days` lookback. Use it after running
`iv-rank-backfill` to recalculate against the newly populated history.

#### Expected Outcome

**Exit code:** `0` on success, `1` on exception.

**stdout (JSON, history available):**

```json
{
  "symbol": "AAPL",
  "lookback_days": 252,
  "current_iv_rank": 65.5,
  "current_iv_percentile": 72.3,
  "current_iv": 0.285,
  "rows": 252,
  "schema": [
    "recorded_at", "symbol", "iv_rank", "iv_percentile",
    "current_iv", "iv_52w_high", "iv_52w_low", "provider"
  ],
  "data": [
    {
      "recorded_at": "2025-04-25",
      "symbol": "AAPL",
      "iv_rank": 45.2,
      "iv_percentile": 52.1,
      "current_iv": 0.245,
      "iv_52w_high": 0.385,
      "iv_52w_low": 0.155,
      "provider": "finnhub"
    }
  ]
}
```

- `current_iv_rank` — today's IV rank (0–100) from the last row in `data`
- `current_iv_percentile` — today's IV percentile (0–100) from the last row
- `current_iv` — today's ATM implied volatility as a decimal (e.g. `0.285` = 28.5%)

**stdout (JSON, no data):** `{"symbol": "AAPL", "rows": 0, "data": []}`

**stderr warning when no data:** `No IV rank data available for AAPL. Check that FINNHUB_API_KEY is set and the symbol has listed options.`

#### Diagnosing a stale / no-history result

The `50.0 / 50.0` fallback and `rows: 1` are the diagnostic markers for "no historical context":

```json
{
  "current_iv_rank": 50.0,
  "current_iv_percentile": 50.0,
  "rows": 1,
  "data": [{ "iv_52w_high": 0.375192, "iv_52w_low": 0.375192 }]
}
```

`iv_52w_high == iv_52w_low == current_iv` confirms only a single data point exists.
`50.0` is **not** a real computed value — it is the code's "no history" sentinel
(`iv_range = 0` → rank defaults to `50.0`; empty history → percentile defaults to `50.0`).

#### IV Rank formula

```
iv_rank       = (current_iv − iv_52w_low) / (iv_52w_high − iv_52w_low) × 100
iv_percentile = % of history days in the lookback window where stored IV < current_iv
```

#### Correct First-Use Workflow

```bash
# Step 1 — backfill at least 1 year of history (one-time, uses Databento API)
market-data iv-rank-backfill --symbol AAPL --start 2025-04-25 --yes

# Expected: JSON with processed=N, skipped=0

# Step 2 — recompute today's rank against the full history
market-data iv-rank --symbol AAPL --force-refresh

# Expected: rows ~252, current_iv_rank and current_iv_percentile are now real values
#           iv_52w_high != iv_52w_low

# Step 3 — normal daily use (no --force-refresh needed)
market-data iv-rank --symbol AAPL

# Expected: same row returned from store; no API call made
```

#### Validation Examples

```bash
# Get 252-day (default) IV rank for AAPL (after backfill)
market-data iv-rank --symbol AAPL

# Expected: rows ~252, current_iv_rank in range [0, 100], iv_52w_high > iv_52w_low

# 2-year lookback (after backfill with --start covering 504 days)
market-data iv-rank --symbol AAPL --days 504

# Expected: rows ~504, lookback_days=504

# Recompute after backfill (clears today's stale 50.0 row)
market-data iv-rank --symbol AAPL --force-refresh

# Expected: current_iv_rank and current_iv_percentile reflect full history

# Check if IV is elevated (>70th percentile — sell premium signal)
market-data iv-rank --symbol AAPL | jq '.current_iv_percentile > 70'

# Expected: true or false (not 50.0 if history is populated)

# CSV export for charting
market-data iv-rank --symbol AAPL --format csv > aapl_iv_history.csv
```

---

### 10. `max-pain` — Compute Max Pain Strike

**Purpose:** Calculate the max pain strike for a specific options expiration. Max pain is
the strike price where option buyers' aggregate losses are maximized — equivalently, where
option writers' aggregate profit is maximized. Computed from stored open interest data.

#### Full Command Syntax

```
market-data max-pain --symbol SYMBOL
                     --expiration YYYY-MM-DD
                     [--snapshot-date YYYY-MM-DD]
                     [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol (e.g. `SPY`) |
| `--expiration` | `-e` | YYYY-MM-DD | **required** | Expiration date |
| `--snapshot-date` | | YYYY-MM-DD | latest available | Which OI snapshot to use |
| `--verbose` | `-v` | flag | false | DEBUG logging + traceback on error |

#### Max Pain Formula

For each candidate strike `S`:
```
pain(S) = Σ_K [ call_OI(K) × max(0, S − K)
              + put_OI(K)  × max(0, K − S) ]
```
`max_pain_price` = the strike `S` that minimises `pain(S)`.

#### Expected Outcome

**Exit code:** `0` on success (including when no data found), `1` on exception.

**stdout (JSON, data found):**

```json
{
  "symbol": "SPY",
  "expiration": "2026-03-20",
  "max_pain_price": 425.0,
  "strikes":    [420.0, 422.5, 425.0, 427.5, 430.0],
  "call_oi":    [45000, 52000, 78000, 61000, 38000],
  "put_oi":     [32000, 48000, 95000, 51000, 21000],
  "total_pain": [1234500, 856300, 125000, 893200, 1456700],
  "snapshot_date": "2026-03-20"
}
```

- `strikes` — list of all strikes included in the calculation
- `call_oi` / `put_oi` — open interest at each corresponding strike
- `total_pain` — computed pain value at each strike; `max_pain_price` is the strike with the lowest value
- `snapshot_date` — the OI snapshot date used

**stdout (JSON, no data):**
```json
{
  "symbol": "SPY",
  "expiration": "2026-03-20",
  "max_pain_price": null,
  "strikes": [],
  "call_oi": [],
  "put_oi": [],
  "total_pain": []
}
```

**stderr warning when no data:** `No options data for SPY expiring 2026-03-20. Run: market-data get --symbol SPY --type options_chain first.`

#### Validation Examples

```bash
# Compute max pain for SPY expiring 2026-03-28 (requires options_chain data to be stored)
market-data max-pain --symbol SPY --expiration 2026-03-28

# Expected: max_pain_price is a strike value; strikes/call_oi/put_oi/total_pain are parallel arrays

# Use a specific historical snapshot
market-data max-pain --symbol SPY --expiration 2026-03-28 --snapshot-date 2026-03-27

# Expected: max_pain_price computed from the 2026-03-27 OI snapshot

# Extract just the max pain price
market-data max-pain --symbol SPY --expiration 2026-03-28 | jq '.max_pain_price'

# Verify strike with minimum total_pain equals max_pain_price
market-data max-pain --symbol SPY --expiration 2026-03-28 | \
  jq 'to_entries | .strikes[(.total_pain | indices(min) | first)]'
```

---

### 11. `validate` — Cross-Validate Data Accuracy

**Purpose:** Cross-validate MDS stored data against independent reference sources. Compares
Alpha Vantage OHLCV and fundamentals against yfinance, and checks Databento OPRA.PILLAR
options for structural integrity. Delegates to `scripts/validate_accuracy.py`.

#### Full Command Syntax

```
market-data validate [--date YYYY-MM-DD]
                     [--report-dir PATH]
                     [--json-only]
                     [--skip-ohlcv]
                     [--skip-fundamentals]
                     [--skip-options]
                     [--verbose]
```

#### Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `--date` | YYYY-MM-DD | most recent trading day | Date to validate |
| `--report-dir` | path | `data/validation` | Directory for JSON reports output |
| `--json-only` | flag | false | Skip Rich table output; write JSON reports only |
| `--skip-ohlcv` | flag | false | Skip OHLCV accuracy checks |
| `--skip-fundamentals` | flag | false | Skip fundamentals accuracy checks |
| `--skip-options` | flag | false | Skip Databento options integrity checks |
| `--verbose` | `-v` | flag | false | DEBUG logging |

#### Expected Outcome

**Exit codes:**

| Code | Meaning |
|------|---------|
| `0` | All checks PASS |
| `1` | WARN — tolerance exceeded but within acceptable threshold |
| `2` | FAIL — significant discrepancies found |
| `3` | ERROR — validation script not found or config error |

**stdout:** Written by `scripts/validate_accuracy.py` (JSON reports to `--report-dir`).

**stderr (exit code 3):** `Validation script not found: <path>`

#### Validation Examples

```bash
# Validate most recent trading day (all checks)
market-data validate

# Expected: exit code 0 if data is accurate; reports written to data/validation/

# Validate a specific date
market-data validate --date 2026-03-27

# Expected: OHLCV, fundamentals, and options checks run against 2026-03-27 data

# Skip expensive options check; output JSON only
market-data validate --skip-options --json-only

# Expected: only OHLCV and fundamentals checks run; no Rich table printed

# Validate to a custom report directory
market-data validate --date 2026-03-27 --report-dir /tmp/validation-reports

# Expected: JSON report files written to /tmp/validation-reports/
```

---

### 12. `iv-rank-backfill` — Backfill Historical IV Rank

**Purpose:** Populate the `iv_rank_history` table for past trading days using Databento
OPRA.PILLAR data. For each trading day, fetches options definitions and OHLCV, computes
ATM IV via Black-Scholes, then calculates and stores `iv_rank` and `iv_percentile`.
Safe to interrupt and re-run — already-stored dates are skipped automatically.

#### Full Command Syntax

```
market-data iv-rank-backfill --symbol SYMBOL
                              --start YYYY-MM-DD
                              [--end YYYY-MM-DD]
                              [--yes]
                              [--verbose]
```

#### Options

| Option | Short | Type | Default | Description |
|--------|-------|------|---------|-------------|
| `--symbol` | `-s` | str | **required** | Ticker symbol (e.g. `SPY`) |
| `--start` | | YYYY-MM-DD | **required** | First date to backfill |
| `--end` | | YYYY-MM-DD | today | Last date to backfill (inclusive) |
| `--yes` | `-y` | flag | false | Skip interactive cost-confirmation prompt |
| `--verbose` | `-v` | flag | false | DEBUG logging + traceback on error |

#### Prerequisites

1. `DATABENTO_API_KEY` must be set in `market-data-service/.env`
2. The Databento provider must be initialized (service checks this on startup)
3. Underlying OHLCV for the symbol must be available (the command auto-fetches it via `get` if not already local)

#### Cost Warning

Each trading day requires **2 Databento API calls** (options definition + ohlcv-1d).
A 1-year backfill (~252 trading days) ≈ **504 API calls**. Without `--yes`, the command
prints an estimate and asks for interactive confirmation.

If all requested dates are already stored, the command outputs a "nothing to do" result
immediately without prompting or making any API calls.

#### Expected Outcome

**Exit code:** `0` on success or "nothing to do", `1` on error.

**stderr pre-run summary:**
```
IV Rank Backfill — SPY
  Date range:      2020-01-01 -> 2026-04-25
  Trading days:    1573
  Already stored:  1321
  To fetch:        252
  Est. API calls:  ~504 (2 per day: definition + ohlcv-1d)

Each Databento API call is billed by data volume. Ensure your plan covers this usage.
Proceed with backfill? [y/N]:
```

**stderr during run:** Rich progress bar (spinner + bar + current date status).

**stderr when nothing to do:** `All dates already stored. Nothing to do.`

**stdout (JSON):**

```json
{
  "symbol": "SPY",
  "start": "2020-01-01",
  "end": "2026-04-25",
  "processed": 252,
  "skipped": 0
}
```

- `processed` — trading days successfully computed and stored
- `skipped` — trading days already in the database (not re-fetched)

**stdout (JSON, nothing to do):**
```json
{"symbol": "SPY", "processed": 0, "skipped": 252}
```

**stderr on missing DATABENTO_API_KEY:**
`Error: Databento provider not initialized. Set DATABENTO_API_KEY in market-data-service/.env`

**stderr when --end < --start:**
`Error: --end must be >= --start`

#### Validation Examples

```bash
# Backfill SPY IV rank from 2020-01-01 to today (interactive confirmation)
market-data iv-rank-backfill --symbol SPY --start 2020-01-01

# Expected: cost summary printed to stderr, confirmation prompt, progress bar, then JSON result

# Backfill a specific range without prompting (CI / automation)
market-data iv-rank-backfill --symbol SPY --start 2019-01-01 --end 2024-12-31 --yes

# Expected: no prompt; backfill runs immediately; JSON result with processed/skipped counts

# Re-run safely (idempotent — all dates already stored)
market-data iv-rank-backfill --symbol SPY --start 2020-01-01 --yes

# Expected: "All dates already stored. Nothing to do." on stderr; {"processed": 0, "skipped": N} on stdout

# Verify IV rank data is now available
market-data iv-rank --symbol SPY

# Expected: rows ~252 (or more), current_iv_rank populated
```

---

## Exit Code Summary

| Code | Meaning | Commands |
|------|---------|----------|
| `0` | Success | All commands |
| `1` | Error / degraded | All commands (also `health` when `overall=false`) |
| `2` | Validation FAIL | `validate` only |
| `3` | Validation script error | `validate` only |

---

## Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `TIMESCALE_URL` | `postgresql://mds:mds_secret@localhost:5432/market_data` | TimescaleDB connection string |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection string |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `mds` | MinIO access key |
| `MINIO_SECRET_KEY` | `mds_secret` | MinIO secret key |
| `MINIO_BUCKET` | `market-data` | MinIO bucket name |
| `MINIO_SECURE` | `false` | Use TLS for MinIO |
| `COVERAGE_DB_PATH` | `/data/manifest/coverage.db` | SQLite coverage manifest path |
| `ALPHA_VANTAGE_API_KEY` | — | Alpha Vantage API key |
| `FINNHUB_API_KEY` | — | Finnhub API key |
| `DATABENTO_API_KEY` | — | Databento API key (required for `iv-rank-backfill`) |
| `TASTYTRADE_CLIENT_ID` | — | TastyTrade OAuth2 client ID |
| `TASTYTRADE_CLIENT_SECRET` | — | TastyTrade OAuth2 client secret |
| `TASTYTRADE_REFRESH_TOKEN` | — | TastyTrade long-lived refresh token |
| `TASTYTRADE_SANDBOX` | `true` | Use TastyTrade sandbox |
| `MDS_CACHE_TTL_REALTIME` | `60` | Redis TTL for tick data (seconds) |
| `MDS_CACHE_TTL_INTRADAY` | `300` | Redis TTL for intraday data (seconds) |
| `MDS_CACHE_TTL_EOD` | `86400` | Redis TTL for end-of-day data (seconds) |
| `MDS_CACHE_TTL_FUNDAMENTALS` | `604800` | Redis TTL for fundamentals (seconds) |
| `MDS_LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |
| `MDS_DRY_RUN` | `false` | Gap analysis only; no provider API calls |
| `MDS_MAX_BATCH_WORKERS` | `4` | Default thread-pool size for `batch` |
| `MDS_REQUEST_TIMEOUT` | `30` | Provider HTTP request timeout (seconds) |
| `MDS_MAX_RETRIES` | `3` | Retry attempts on transient provider failures |
