"""
market_data/models.py
Unified data models — the canonical schema every provider normalizes to.
"""

import warnings

from pydantic import BaseModel, Field
from enum import Enum
from typing import Optional, Any
from datetime import date, datetime

# "schema" is intentionally used as a field name (column-list contract for CLI consumers).
# It shadows BaseModel.schema() which is deprecated in Pydantic v2 anyway.
warnings.filterwarnings(
    "ignore",
    message=r'Field name "schema" .* shadows an attribute in parent "BaseModel"',
    category=UserWarning,
)


# ── Enums ──────────────────────────────────────────────────────────────────

class DataType(str, Enum):
    OHLCV = "ohlcv"
    OHLCV_INTRADAY = "ohlcv_intraday"
    OPTIONS_CHAIN = "options_chain"
    FUNDAMENTALS = "fundamentals"
    NEWS_SENTIMENT = "news_sentiment"
    EARNINGS = "earnings"
    DIVIDENDS = "dividends"
    IV_RANK = "iv_rank"
    TICK = "tick"
    FUTURES_OHLCV = "futures_ohlcv"

    def __str__(self) -> str:  # Python 3.11+ compatibility with Typer
        return self.value


class Interval(str, Enum):
    ONE_MIN = "1m"
    FIVE_MIN = "5m"
    FIFTEEN_MIN = "15m"
    ONE_HOUR = "1h"
    FOUR_HOUR = "4h"
    ONE_DAY = "1d"
    ONE_WEEK = "1w"

    def __str__(self) -> str:  # Python 3.11+ compatibility with Typer
        return self.value


class ProviderName(str, Enum):
    ALPHA_VANTAGE = "alpha_vantage"
    FINNHUB = "finnhub"
    DATABENTO = "databento"
    CACHE = "cache"
    TIMESCALEDB = "timescaledb"
    MERGED = "merged"


class CoverageStatus(str, Enum):
    COMPLETE = "complete"
    PARTIAL = "partial"
    MISSING = "missing"


class OptionType(str, Enum):
    CALL = "call"
    PUT = "put"


# ── Core Row Models ────────────────────────────────────────────────────────

class OHLCVRow(BaseModel):
    timestamp: datetime
    symbol: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adj_close: Optional[float] = None
    provider: str = ""


class OHLCVIntradayRow(OHLCVRow):
    interval: Interval = Interval.ONE_DAY


class FundamentalsRow(BaseModel):
    snapshot_date: date
    symbol: str
    pe_ratio: Optional[float] = None
    eps: Optional[float] = None
    revenue: Optional[int] = None
    market_cap: Optional[int] = None
    debt_to_equity: Optional[float] = None
    roe: Optional[float] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    raw_data: Optional[dict] = None
    provider: str = ""


class EarningsRow(BaseModel):
    report_date: date
    symbol: str
    eps_actual: Optional[float] = None
    eps_estimate: Optional[float] = None
    eps_surprise: Optional[float] = None
    revenue_actual: Optional[int] = None
    revenue_estimate: Optional[int] = None
    fiscal_quarter: Optional[str] = None
    fiscal_year: Optional[int] = None
    provider: str = ""


class NewsSentimentRow(BaseModel):
    published_at: datetime
    symbol: str
    headline: str
    source: Optional[str] = None
    sentiment_score: Optional[float] = None  # -1.0 to 1.0
    sentiment_label: Optional[str] = None    # positive/neutral/negative
    url: Optional[str] = None
    provider: str = ""


class OptionsRow(BaseModel):
    """
    A single option contract row from a chain snapshot.

    Greeks (delta, gamma, theta, vega, rho) are as provided by the data source.
    underlying_price captures the spot price at snapshot time, needed for
    moneyness calculations and strategy construction.
    iv_rank and iv_percentile are rolling volatility metrics where available.
    """
    snapshot_at: datetime
    symbol: str
    expiration_date: date
    strike: float
    option_type: OptionType
    bid: Optional[float] = None
    ask: Optional[float] = None
    last: Optional[float] = None
    volume: Optional[int] = None
    open_interest: Optional[int] = None
    implied_volatility: Optional[float] = None
    delta: Optional[float] = None
    gamma: Optional[float] = None
    theta: Optional[float] = None
    vega: Optional[float] = None
    rho: Optional[float] = None               # Rate sensitivity Greek
    iv_rank: Optional[float] = None           # 0-100: IV vs 52-week range
    iv_percentile: Optional[float] = None     # % of days with lower IV in lookback
    underlying_price: Optional[float] = None  # Spot price at snapshot time
    provider: str = ""


class IVRankRow(BaseModel):
    """
    Daily IV rank snapshot for a symbol.

    Stored separately from options snapshots so strategies can efficiently
    query IV rank history without loading full option chains. Essential for
    all volatility-mean-reversion and premium-selling strategies.

    iv_rank: 0-100, where current IV sits vs its 52-week high/low range.
      - iv_rank = (current_iv - iv_52w_low) / (iv_52w_high - iv_52w_low) * 100
    iv_percentile: 0-100, what % of trading days in the lookback had lower IV.
      - iv_rank and iv_percentile diverge when IV distribution is skewed.
    """
    recorded_at: date
    symbol: str
    iv_rank: float           # 0-100: current IV vs 52w range
    iv_percentile: float     # 0-100: % of days with lower IV
    current_iv: float        # Current ATM implied volatility
    iv_52w_high: float       # 52-week IV high
    iv_52w_low: float        # 52-week IV low
    provider: str = ""


class DividendRow(BaseModel):
    ex_date: date
    symbol: str
    amount: float
    pay_date: Optional[date] = None
    declaration_date: Optional[date] = None
    provider: str = ""


# ── Coverage Models ────────────────────────────────────────────────────────

class DateGap(BaseModel):
    start: date
    end: date

    def __str__(self) -> str:
        return f"{self.start} → {self.end}"


class CoverageRecord(BaseModel):
    symbol: str
    data_type: DataType
    interval: Optional[Interval] = None
    start_date: date
    end_date: date
    provider: str
    row_count: Optional[int] = None
    fetched_at: Optional[datetime] = None


class CoverageReport(BaseModel):
    symbol: str
    data_type: DataType
    interval: Optional[Interval] = None
    requested_start: date
    requested_end: date
    status: CoverageStatus
    gaps: list[DateGap] = Field(default_factory=list)
    covered_ranges: list[tuple[date, date]] = Field(default_factory=list)


# ── Response Models (CLI stdout contract) ─────────────────────────────────

class DataResponse(BaseModel):
    model_config = {"populate_by_name": True}

    symbol: str
    data_type: DataType
    interval: Optional[str] = None
    source: str                           # cache / timescaledb / api:alpha_vantage / merged
    coverage: CoverageStatus
    gaps: list[DateGap] = Field(default_factory=list)
    rows: int
    fetched_at: str
    schema: list[str] = Field(default_factory=list)   # column names; shadows BaseModel.schema (intentional)
    data: list[dict[str, Any]]


class StatusResponse(BaseModel):
    symbol: str
    data_type: DataType
    coverage: CoverageStatus
    available_ranges: list[dict[str, str]]
    gaps: list[DateGap]
    total_rows: Optional[int] = None


class HealthResponse(BaseModel):
    timescaledb: bool
    redis: bool
    minio: bool
    providers: dict[str, bool]
    overall: bool


class BatchResponse(BaseModel):
    requested: list[str]
    succeeded: list[str]
    failed: list[str]
    results: dict[str, DataResponse]
