## WARNING: This is a new project and needs some validation work before it is fully ready. Expect issues. I am working to stabilize quickly.

# Market Data Service

Shared market data infrastructure for the agent trading firm. All trading agents fetch data through this service — no agent calls Alpha Vantage, Finnhub, or Databento directly.

## How It Works

```
Agent → CLI / Python client → Coverage Check → Gap Detect → Delta Fetch → Local Store → Return
```

On the first request for a symbol/date range, data is fetched from the provider and stored locally. All subsequent requests for overlapping ranges are served from local cache — zero API calls. Only missing gaps trigger upstream fetches.

## Quickstart

### 1. Start Infrastructure

```bash
cp .env.example .env
# Add your API keys to .env

make up
```

### 2. Install

```bash
make install
```

### 3. Verify

```bash
make health
```

### 4. Fetch Data

```bash
# Fetch 1 year of AAPL OHLCV (first call hits API, subsequent calls from local DB)
market-data get --symbol AAPL --type ohlcv --days 365

# Pre-populate a watchlist
market-data warm --watchlist watchlist.txt --days 365

# Check what's stored locally
market-data list --symbol AAPL

# Batch fetch multiple symbols
market-data batch --symbols AAPL,TSLA,SPY,QQQ --type ohlcv --days 365
```

## CLI Reference

### `get` — Fetch data (cache-first)
```bash
market-data get \
  --symbol AAPL \
  --type ohlcv \          # ohlcv | ohlcv_intraday | options_chain | fundamentals | news_sentiment | earnings | dividends | tick | futures_ohlcv
  --start 2024-01-01 \    # or use --days N
  --end 2025-01-01 \
  --interval 1d \         # 1m | 5m | 15m | 1h | 4h | 1d | 1w
  --format json \         # json | csv
  --force-refresh \       # bypass cache
  --provider finnhub      # override default provider
```

**stdout contract (JSON):**
```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "source": "timescaledb",
  "coverage": "complete",
  "gaps": [],
  "rows": 252,
  "fetched_at": "2026-03-17T09:00:00Z",
  "schema": ["timestamp", "symbol", "open", "high", "low", "close", "volume"],
  "data": [...]
}
```

### `status` — Check local coverage without fetching
```bash
market-data status --symbol AAPL --type ohlcv --days 365
```

### `batch` — Parallel multi-symbol fetch
```bash
market-data batch --symbols AAPL,TSLA,SPY --type ohlcv --days 365 --workers 4
```

### `warm` — Pre-populate a watchlist
```bash
# From file (one symbol per line, # for comments)
market-data warm --watchlist watchlist.txt --days 365 --types ohlcv,fundamentals

# Inline
market-data warm --watchlist "AAPL,TSLA,SPY,QQQ,IWM" --days 90
```

### `health` — Infrastructure status
```bash
market-data health
```

### `audit` — Data quality report
```bash
market-data audit --symbol AAPL --type ohlcv --days 365
```

## Agent Integration

### Option A: CLI subprocess (cross-language, any agent)

```python
import subprocess, json
import pandas as pd

def get_ohlcv(symbol: str, days: int = 365) -> pd.DataFrame:
    result = subprocess.run(
        ["market-data", "get",
         "--symbol", symbol,
         "--type", "ohlcv",
         "--days", str(days),
         "--format", "json"],
        capture_output=True, text=True, check=True
    )
    resp = json.loads(result.stdout)
    df = pd.DataFrame(resp["data"], columns=resp["schema"])
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    return df

# Usage in any agent
df = get_ohlcv("AAPL")
```

### Option B: Python client (in-process Python agents)

```python
from market_data.client import MarketDataClient

mds = MarketDataClient()

# Single symbol
df = mds.get("AAPL", "ohlcv", days=365)

# Multiple symbols
dfs = mds.batch(["SPY", "QQQ", "IWM"], "ohlcv", days=90)
aapl_df = dfs["SPY"]

# Fundamentals
fund_df = mds.get("AAPL", "fundamentals")

# Options chain
opts_df = mds.get("SPY", "options_chain")

# Coverage check without fetching
status = mds.status("AAPL", "ohlcv")
print(status["coverage"])  # complete | partial | missing
```

## Data Types

| Type              | Description                     | Free Providers              |
|-------------------|---------------------------------|-----------------------------|
| `ohlcv`           | Daily OHLCV bars                | Alpha Vantage, Finnhub      |
| `ohlcv_intraday`  | Intraday bars (1m/5m/15m/1h)   | Alpha Vantage, Finnhub      |
| `options_chain`   | Full options chain snapshot     | Finnhub                     |
| `fundamentals`    | P/E, EPS, revenue, market cap   | Alpha Vantage, Finnhub      |
| `news_sentiment`  | News headlines + sentiment      | Finnhub, Alpha Vantage      |
| `earnings`        | Historical earnings data        | Alpha Vantage, Finnhub      |
| `dividends`       | Historical dividends            | Alpha Vantage               |
| `tick`            | Raw trade ticks                 | Databento (paid)            |
| `futures_ohlcv`   | Futures OHLCV (CME)             | Databento (paid)            |

## Provider Rate Limits

| Provider       | Free Limit       | Strategy                          |
|----------------|------------------|-----------------------------------|
| Alpha Vantage  | 25 calls/day     | Token bucket, daily quota in Redis|
| Finnhub        | 60 calls/min     | Token bucket, 1.1s min interval   |
| Databento      | Usage-based (paid)| Batch aggressively, archive all   |

## Storage Architecture

| Layer        | Technology  | Contents                    | Access Speed |
|--------------|-------------|------------------------------|--------------|
| Hot cache    | Redis       | Last 24h quotes, intraday    | < 1ms        |
| Time-series  | TimescaleDB | All OHLCV, earnings, options | < 10ms       |
| Cold archive | MinIO/Parquet| Tick data, bulk history      | < 100ms      |
| Manifest     | SQLite      | What we have and when        | < 1ms        |

## Development

```bash
make test          # Run unit tests
make lint          # Lint check
make typecheck     # Type check
make test-integration  # Real API tests (requires keys)
```

## Project Structure

```
market-data-service/
├── market_data/
│   ├── cli.py              # Typer CLI (agent interface)
│   ├── client.py           # Python client wrapper
│   ├── service.py          # Core orchestration
│   ├── config.py           # Pydantic settings
│   ├── models.py           # Shared data models
│   ├── cache/
│   │   ├── coverage.py     # SQLite coverage manifest
│   │   └── redis_cache.py  # Redis hot cache
│   ├── providers/
│   │   ├── base.py         # Abstract base + rate limiter
│   │   ├── alpha_vantage.py
│   │   ├── finnhub.py
│   │   ├── databento.py
│   │   └── router.py       # Provider selection
│   ├── storage/
│   │   └── timescale.py    # TimescaleDB client
│   └── utils/
│       └── date_utils.py   # Market calendar helpers
├── migrations/
│   └── 001_initial_schema.sql
├── tests/
│   ├── unit/               # No API calls
│   └── integration/        # Real API calls (gated)
├── docker-compose.yml
├── pyproject.toml
├── Makefile
└── .env.example
```
