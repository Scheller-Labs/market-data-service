# Market Data Service — Project Plan
## Agent Trading Firm Infrastructure

---

## 1. Purpose and Goals

The Market Data Service (MDS) is a shared infrastructure layer that sits between all trading agents and external data providers. No agent calls Alpha Vantage, Finnhub, or Databento directly. Every data request goes through the MDS CLI, which handles caching, gap detection, provider routing, and unified schema delivery.

**Primary Goals:**
- Eliminate redundant API calls across agents
- Enforce data consistency — all agents see the same data for the same symbol/date range
- Abstract provider details — agents never know which API sourced the data
- Enable reproducible backtests via frozen local data snapshots
- Control costs on paid providers (Databento especially)

---

## 2. System Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│                     TRADING AGENTS (N)                           │
│  Technical  │  Fundamental  │  Sentiment  │  Risk  │  Execution  │
│                                                                  │
│  market-data get --symbol AAPL --type ohlcv --days 365           │
└────────────────────────────┬─────────────────────────────────────┘
                             │  CLI / JSON stdout
                             ▼
┌──────────────────────────────────────────────────────────────────┐
│                  MARKET DATA SERVICE (MDS)                       │
│                                                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────────┐  │
│  │  CLI Layer  │  │   Service   │  │   Provider Router       │  │
│  │  (Typer)    │→ │  Orchestr.  │→ │   Alpha Vantage         │  │
│  └─────────────┘  └──────┬──────┘  │   Finnhub               │  │
│                          │         │   Databento             │  │
│                    ┌─────┴─────┐   └─────────────────────────┘  │
│                    │  Coverage │                                  │
│                    │  Manifest │ (SQLite — what do we have?)     │
│                    └─────┬─────┘                                  │
└──────────────────────────┼───────────────────────────────────────┘
                           │
          ┌────────────────┼──────────────────┐
          ▼                ▼                  ▼
   ┌─────────────┐  ┌─────────────┐  ┌──────────────────┐
   │    Redis    │  │ TimescaleDB │  │  MinIO (Parquet)  │
   │  (hot cache │  │ (structured │  │  (cold archive   │
   │   <24h)     │  │  OHLCV etc) │  │   >1yr data)     │
   └─────────────┘  └─────────────┘  └──────────────────┘
```

---

## 3. Data Types Supported

| Type Key           | Description                          | Sources                          |
|--------------------|--------------------------------------|----------------------------------|
| `ohlcv`            | Daily open/high/low/close/volume     | Alpha Vantage, Databento, Finnhub|
| `ohlcv_intraday`   | 1m/5m/15m/1h bars                    | Databento, Alpha Vantage         |
| `options_chain`    | Full options chain snapshot          | Databento, Finnhub               |
| `fundamentals`     | P/E, EPS, revenue, balance sheet     | Alpha Vantage, Finnhub           |
| `news_sentiment`   | News headlines + sentiment scores    | Finnhub, Alpha Vantage           |
| `earnings`         | Historical + upcoming earnings dates | Alpha Vantage, Finnhub           |
| `dividends`        | Historical dividend data             | Alpha Vantage                    |
| `iv_rank`          | Historical IV rank / IV percentile   | Databento, Finnhub               |
| `tick`             | Raw tick data                        | Databento only                   |
| `futures_ohlcv`    | Futures contract OHLCV               | Databento                        |

---

## 4. Provider Capability Matrix

| Capability         | Alpha Vantage   | Finnhub         | Databento       |
|--------------------|-----------------|-----------------|-----------------|
| OHLCV EOD          | ✅ Free (25/day) | ✅ Free (60/min) | ✅ Paid          |
| OHLCV Intraday     | ✅ Free (25/day) | ✅ Free          | ✅ Paid          |
| Options Chain      | ❌               | ✅ Free          | ✅ Paid          |
| Fundamentals       | ✅ Free          | ✅ Free          | ❌               |
| News/Sentiment     | ✅ Free          | ✅ Free          | ❌               |
| Tick Data          | ❌               | ❌               | ✅ Paid          |
| Futures            | ❌               | ❌               | ✅ Paid          |
| IV / Greeks        | ❌               | ✅ Free          | ✅ Paid          |
| Rate Limit         | 25 calls/day    | 60 calls/min    | Usage-based     |

---

## 5. Storage Layer Design

### 5a. Redis — Hot Cache (< 24h)
- **Purpose**: Sub-millisecond reads for agents running in tight loops
- **Key pattern**: `mds:{type}:{symbol}:{interval}:{date}`
- **TTL**: 
  - Real-time quotes: 60 seconds
  - Intraday bars: 5 minutes
  - EOD data: until next market open
- **Contents**: Latest N rows as compressed JSON

### 5b. TimescaleDB — Warm Store (structured time-series)
- **Purpose**: Primary query target for all structured OHLCV, fundamentals, earnings
- **Hypertable**: Partitioned by time, indexed by symbol
- **Retention**: Indefinite (all history)
- **Tables**: `ohlcv_daily`, `ohlcv_intraday`, `fundamentals`, `options_snapshots`, `news_sentiment`, `earnings_history`

### 5c. MinIO / Parquet — Cold Archive (bulk/tick data)
- **Purpose**: Tick data, large bulk downloads, Databento raw responses
- **Format**: Parquet files partitioned by `symbol/year/month/`
- **Access**: Via DuckDB queries, not directly by agents
- **Lifecycle**: Compress to Zstandard after 90 days

### 5d. SQLite — Coverage Manifest
- **Purpose**: Fast lookup of what data we have locally before touching any API
- **Tables**: `coverage_map (symbol, data_type, start_date, end_date, provider, fetched_at, row_count)`
- **Location**: `/data/manifest/coverage.db`

---

## 6. CLI Command Reference (Agent Interface)

```bash
# --- Core Commands ---

# Get data (cache-first, delta-fetch)
market-data get \
  --symbol AAPL \
  --type ohlcv \
  --start 2024-01-01 \
  --end 2025-01-01 \
  --interval 1d \
  --format json          # json | csv | parquet

# Check local coverage without fetching
market-data status \
  --symbol AAPL \
  --type ohlcv \
  --start 2024-01-01

# Batch fetch for multiple symbols
market-data batch \
  --symbols AAPL,TSLA,SPY,QQQ \
  --type ohlcv \
  --days 365

# Force refresh from upstream provider
market-data get --symbol AAPL --type ohlcv --force-refresh

# List all locally available data
market-data list --type ohlcv
market-data list --symbol AAPL

# Provider health check
market-data health

# Data quality report for a symbol
market-data audit --symbol AAPL --type ohlcv

# Warm cache (pre-fetch a watchlist)
market-data warm --watchlist watchlist.txt --days 90
```

### Stdout Contract (agents parse this)
```json
{
  "symbol": "AAPL",
  "data_type": "ohlcv",
  "interval": "1d",
  "source": "cache|timescaledb|api:alpha_vantage|merged",
  "coverage": "complete|partial",
  "gaps": [],
  "rows": 252,
  "fetched_at": "2026-03-17T09:00:00Z",
  "schema": ["timestamp", "open", "high", "low", "close", "volume"],
  "data": [...]
}
```

---

## 7. Project Structure

```
market-data-service/
├── docker-compose.yml           # TimescaleDB, Redis, MinIO containers
├── .env.example                 # All env vars documented
├── pyproject.toml               # Package + deps (Poetry)
├── Makefile                     # Dev shortcuts
├── README.md
│
├── market_data/                 # Main Python package
│   ├── __init__.py
│   ├── cli.py                   # Typer CLI entry point
│   ├── config.py                # Pydantic settings (env-backed)
│   ├── models.py                # Shared Pydantic data models
│   │
│   ├── cache/
│   │   ├── __init__.py
│   │   ├── redis_cache.py       # Redis read/write/TTL logic
│   │   └── coverage.py          # SQLite coverage manifest
│   │
│   ├── storage/
│   │   ├── __init__.py
│   │   ├── timescale.py         # TimescaleDB upsert/query
│   │   └── minio_store.py       # MinIO Parquet archive
│   │
│   ├── providers/
│   │   ├── __init__.py
│   │   ├── base.py              # Abstract base + RateLimiter
│   │   ├── alpha_vantage.py     # Alpha Vantage adapter
│   │   ├── finnhub.py           # Finnhub adapter
│   │   ├── databento.py         # Databento adapter
│   │   └── router.py            # Provider selection + fallback
│   │
│   ├── service.py               # Core orchestration (get/status/batch)
│   └── utils/
│       ├── __init__.py
│       ├── date_utils.py        # Gap calculation, market calendars
│       ├── rate_limiter.py      # Token bucket per provider
│       └── normalizer.py        # Normalize all provider schemas → MDS schema
│
├── migrations/
│   ├── 001_initial_schema.sql   # TimescaleDB tables + hypertables
│   └── 002_indexes.sql          # Performance indexes
│
├── scripts/
│   ├── warm_cache.py            # Pre-populate from a watchlist
│   └── backfill.py              # Bulk historical backfill
│
└── tests/
    ├── conftest.py
    ├── unit/
    │   ├── test_coverage.py
    │   ├── test_gap_detection.py
    │   ├── test_normalizer.py
    │   └── test_router.py
    └── integration/
        ├── test_alpha_vantage.py
        ├── test_finnhub.py
        └── test_service.py
```

---

## 8. Build Phases

### Phase 1 — Core Infrastructure (Days 1–2)
- [ ] Docker Compose with TimescaleDB, Redis, MinIO
- [ ] TimescaleDB schema + hypertables
- [ ] SQLite coverage manifest schema
- [ ] Pydantic config (env-backed)
- [ ] Unified data models

**Exit Criteria**: `docker-compose up` starts all services cleanly. Coverage manifest initialized.

---

### Phase 2 — Storage Layer (Days 2–3)
- [ ] TimescaleDB read/write client (upsert-safe)
- [ ] Redis cache client with TTL management
- [ ] MinIO Parquet archive writer
- [ ] Coverage manifest read/write/gap-detect

**Exit Criteria**: Can write OHLCV rows to TimescaleDB and query them back. Coverage manifest correctly reports what's available.

---

### Phase 3 — Provider Adapters (Days 3–5)
- [ ] Abstract base provider + rate limiter
- [ ] Alpha Vantage adapter (OHLCV, fundamentals, earnings, news)
- [ ] Finnhub adapter (OHLCV, options chain, IV, news)
- [ ] Databento adapter (OHLCV, tick, futures, options)
- [ ] Schema normalizer (all providers → MDS unified schema)
- [ ] Provider router (capability map + fallback chain)

**Exit Criteria**: Each adapter fetches AAPL OHLCV and produces a normalized DataFrame with identical column names. Rate limits respected.

---

### Phase 4 — Core Service Orchestration (Days 5–6)
- [ ] `get()` — coverage check → gap analysis → delta fetch → merge → return
- [ ] `status()` — coverage report without fetching
- [ ] `batch()` — parallel fetch for multiple symbols
- [ ] `warm()` — pre-populate a watchlist
- [ ] Error handling, retry logic, provider fallback

**Exit Criteria**: `get(AAPL, ohlcv, 2yr)` fetches from API on first call, returns fully from cache on second call. Gap in middle causes partial re-fetch only.

---

### Phase 5 — CLI Layer (Day 6–7)
- [ ] Typer CLI wiring for all commands
- [ ] JSON / CSV / Parquet output formatters
- [ ] `--format json` stdout for agent consumption
- [ ] `--verbose` mode for human debugging
- [ ] Shell completion generation

**Exit Criteria**: `market-data get --symbol AAPL --type ohlcv --days 365 --format json` returns valid JSON to stdout. Agents can pipe this directly.

---

### Phase 6 — Quality, Tests, Docs (Days 7–8)
- [ ] Unit tests for gap detection, coverage manifest, normalizer, router
- [ ] Integration tests with real API calls (gated by env flag)
- [ ] `make test`, `make lint`, `make typecheck` all pass
- [ ] README with quickstart, CLI reference, agent integration examples
- [ ] Docker Compose health checks

**Exit Criteria**: `make test` passes. `make lint` clean. README covers all agent use cases.

---

## 9. Agent Integration Pattern

Agents invoke the CLI as a subprocess and parse JSON from stdout:

```python
# In any agent
import subprocess, json

def get_market_data(symbol: str, data_type: str, days: int) -> dict:
    result = subprocess.run(
        ["market-data", "get",
         "--symbol", symbol,
         "--type", data_type,
         "--days", str(days),
         "--format", "json"],
        capture_output=True, text=True, check=True
    )
    return json.loads(result.stdout)

# Usage
data = get_market_data("AAPL", "ohlcv", 365)
df = pd.DataFrame(data["data"], columns=data["schema"])
```

Or via a thin Python client wrapper (included in `market_data.client`):

```python
from market_data.client import MarketDataClient

mds = MarketDataClient()
df = mds.get("AAPL", "ohlcv", days=365)          # returns DataFrame
status = mds.status("AAPL", "ohlcv")              # coverage report
mds.batch(["SPY", "QQQ", "IWM"], "ohlcv", days=90)
```

---

## 10. Environment Variables

```bash
# Provider API Keys
ALPHA_VANTAGE_API_KEY=
FINNHUB_API_KEY=
DATABENTO_API_KEY=

# Storage
TIMESCALE_URL=postgresql://mds:mds@localhost:5432/market_data
REDIS_URL=redis://localhost:6379/0
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=mds
MINIO_SECRET_KEY=mds_secret
MINIO_BUCKET=market-data

# Coverage Manifest
COVERAGE_DB_PATH=/data/manifest/coverage.db

# Behavior
MDS_DEFAULT_PROVIDER_OHLCV=alpha_vantage     # override per type
MDS_CACHE_TTL_EOD=86400                       # seconds
MDS_CACHE_TTL_INTRADAY=300
MDS_LOG_LEVEL=INFO
MDS_DRY_RUN=false                             # log what would be fetched, don't call APIs
```

---

## 11. Rate Limit Management

| Provider       | Limit            | Strategy                                      |
|----------------|------------------|-----------------------------------------------|
| Alpha Vantage  | 25 calls/day     | Token bucket, daily quota tracker in Redis    |
| Finnhub        | 60 calls/min     | Token bucket, 1s inter-call floor             |
| Databento      | Usage-based      | Request batching, minimize calls per symbol   |

The rate limiter is enforced at the provider adapter level and will raise `RateLimitExceeded` with a human-readable message indicating when the next call is permitted.

---

## 12. Dependency List

```toml
[tool.poetry.dependencies]
python = "^3.11"
typer = "^0.12"
pydantic = "^2.0"
pydantic-settings = "^2.0"
psycopg2-binary = "^2.9"
sqlalchemy = "^2.0"
redis = "^5.0"
minio = "^7.2"
pandas = "^2.0"
pyarrow = "^15.0"
httpx = "^0.27"            # async HTTP for provider calls
tenacity = "^8.2"          # retry logic
exchange-calendars = "^4.5" # market open/close days
databento = "^0.35"        # official Databento SDK
rich = "^13.0"             # CLI output formatting
```
