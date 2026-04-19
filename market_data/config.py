"""
market_data/config.py
Configuration management — all settings pulled from environment variables.
Uses pydantic-settings v2 with validation_alias for non-standard env var names.
"""

from enum import Enum
from typing import Optional

from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# Resolve .env relative to this file's parent (market-data-service root),
# so it works regardless of the caller's working directory.
_ENV_FILE = Path(__file__).parent.parent / ".env"


class LogLevel(str, Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"


class OutputFormat(str, Enum):
    JSON = "json"
    CSV = "csv"
    PARQUET = "parquet"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Provider API Keys ──────────────────────────────────────────
    # These map directly: field name = env var name (case-insensitive)
    alpha_vantage_api_key: Optional[str] = Field(None)
    finnhub_api_key: Optional[str] = Field(None)
    databento_api_key: Optional[str] = Field(None)
    tastytrade_client_id: Optional[str] = Field(None)
    tastytrade_client_secret: Optional[str] = Field(None)
    tastytrade_refresh_token: Optional[str] = Field(None)
    tastytrade_sandbox: bool = Field(True)

    # ── Storage ────────────────────────────────────────────────────
    timescale_url: str = Field(
        "postgresql://mds:mds_secret@localhost:5432/market_data"
    )
    redis_url: str = Field("redis://localhost:6379/0")
    minio_endpoint: str = Field("localhost:9000")
    minio_access_key: str = Field("mds")
    minio_secret_key: str = Field("mds_secret")
    minio_bucket: str = Field("market-data")
    minio_secure: bool = Field(False)

    # ── Coverage Manifest ──────────────────────────────────────────
    coverage_db_path: str = Field("/data/manifest/coverage.db")

    # ── Provider Defaults ──────────────────────────────────────────
    # These env vars have MDS_ prefix, so we use validation_alias to map them
    default_provider_ohlcv: str = Field(
        "alpha_vantage",
        validation_alias=AliasChoices("MDS_DEFAULT_PROVIDER_OHLCV", "default_provider_ohlcv"),
    )
    default_provider_options: str = Field(
        "finnhub",
        validation_alias=AliasChoices("MDS_DEFAULT_PROVIDER_OPTIONS", "default_provider_options"),
    )
    default_provider_fundamentals: str = Field(
        "alpha_vantage",
        validation_alias=AliasChoices("MDS_DEFAULT_PROVIDER_FUNDAMENTALS", "default_provider_fundamentals"),
    )
    default_provider_news: str = Field(
        "finnhub",
        validation_alias=AliasChoices("MDS_DEFAULT_PROVIDER_NEWS", "default_provider_news"),
    )
    default_provider_tick: str = Field(
        "databento",
        validation_alias=AliasChoices("MDS_DEFAULT_PROVIDER_TICK", "default_provider_tick"),
    )

    # ── Cache TTLs (seconds) ───────────────────────────────────────
    cache_ttl_realtime: int = Field(
        60,
        validation_alias=AliasChoices("MDS_CACHE_TTL_REALTIME", "cache_ttl_realtime"),
    )
    cache_ttl_intraday: int = Field(
        300,
        validation_alias=AliasChoices("MDS_CACHE_TTL_INTRADAY", "cache_ttl_intraday"),
    )
    cache_ttl_eod: int = Field(
        86400,
        validation_alias=AliasChoices("MDS_CACHE_TTL_EOD", "cache_ttl_eod"),
    )
    cache_ttl_fundamentals: int = Field(
        86400 * 7,
        validation_alias=AliasChoices("MDS_CACHE_TTL_FUNDAMENTALS", "cache_ttl_fundamentals"),
    )

    # ── Behavior ───────────────────────────────────────────────────
    log_level: LogLevel = Field(
        LogLevel.INFO,
        validation_alias=AliasChoices("MDS_LOG_LEVEL", "log_level"),
    )
    dry_run: bool = Field(
        False,
        validation_alias=AliasChoices("MDS_DRY_RUN", "dry_run"),
    )
    max_batch_workers: int = Field(
        4,
        validation_alias=AliasChoices("MDS_MAX_BATCH_WORKERS", "max_batch_workers"),
    )
    request_timeout_seconds: int = Field(
        30,
        validation_alias=AliasChoices("MDS_REQUEST_TIMEOUT", "request_timeout_seconds"),
    )
    max_retries: int = Field(
        3,
        validation_alias=AliasChoices("MDS_MAX_RETRIES", "max_retries"),
    )
    databento_require_confirmation: bool = Field(
        True,
        validation_alias=AliasChoices("MDS_DATABENTO_REQUIRE_CONFIRMATION", "databento_require_confirmation"),
    )


# Singleton — import this everywhere
settings = Settings()
