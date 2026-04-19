"""
market_data/storage/minio_store.py
MinIO object store client — Parquet archive for bulk and tick data.

Cold archive layer: data older than 90 days moves here from TimescaleDB.
Format: Parquet files partitioned by symbol/year/month/.
All Parquet files are compressed with Snappy (pyarrow default).
Access pattern: DuckDB queries via query_parquet(), not raw file reads.
"""

import io
import logging
from datetime import date
from pathlib import PurePosixPath
from typing import Optional

import pandas as pd

from market_data.config import settings
from market_data.models import DataType

logger = logging.getLogger(__name__)


class MinIOStore:
    """
    Parquet archive client backed by MinIO object storage.

    Partition layout:
        market-data/{data_type}/{symbol}/{year}/{month:02d}/data.parquet

    This layout enables efficient time-range queries via DuckDB's
    Hive partitioning support and minimizes data scanned per request.
    """

    def __init__(
        self,
        endpoint: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        bucket: Optional[str] = None,
        secure: Optional[bool] = None,
    ):
        self._endpoint = endpoint or settings.minio_endpoint
        self._access_key = access_key or settings.minio_access_key
        self._secret_key = secret_key or settings.minio_secret_key
        self._bucket = bucket or settings.minio_bucket
        self._secure = secure if secure is not None else settings.minio_secure
        self._client = None

    def _get_client(self):
        """Lazy-init MinIO client to defer import and connection."""
        if self._client is None:
            try:
                from minio import Minio
                self._client = Minio(
                    self._endpoint,
                    access_key=self._access_key,
                    secret_key=self._secret_key,
                    secure=self._secure,
                )
                self._ensure_bucket()
            except ImportError:
                raise RuntimeError("minio package not installed. Run: pip install minio")
        return self._client

    def _ensure_bucket(self) -> None:
        """Create bucket if it doesn't exist."""
        try:
            client = self._client
            if not client.bucket_exists(self._bucket):
                client.make_bucket(self._bucket)
                logger.info(f"Created MinIO bucket: {self._bucket}")
        except Exception as e:
            logger.warning(f"Could not ensure bucket exists: {e}")

    # ── Object Key Construction ────────────────────────────────────────────

    def _make_key(
        self,
        data_type: DataType,
        symbol: str,
        year: int,
        month: int,
    ) -> str:
        """
        Build the object storage key for a given partition.

        Example: ohlcv/AAPL/2024/01/data.parquet
        """
        return str(PurePosixPath(
            data_type.value,
            symbol.upper(),
            str(year),
            f"{month:02d}",
            "data.parquet",
        ))

    def _make_prefix(
        self,
        data_type: DataType,
        symbol: str,
        year: Optional[int] = None,
    ) -> str:
        """Build prefix for listing objects (symbol or symbol/year level)."""
        parts = [data_type.value, symbol.upper()]
        if year:
            parts.append(str(year))
        return str(PurePosixPath(*parts)) + "/"

    # ── Write ──────────────────────────────────────────────────────────────

    def write_parquet(
        self,
        df: pd.DataFrame,
        data_type: DataType,
        symbol: str,
        year: int,
        month: int,
    ) -> str:
        """
        Write a DataFrame to MinIO as a Parquet file for a given month partition.

        If a file already exists for this partition, the new data is merged
        (union + dedup on the first column, assumed to be the timestamp/date key).

        Args:
            df: DataFrame to persist.
            data_type: Data type key (used in object path).
            symbol: Ticker symbol.
            year: Calendar year for partitioning.
            month: Calendar month for partitioning.

        Returns:
            The object key that was written.
        """
        if df.empty:
            logger.debug(f"Skipping empty DataFrame write for {symbol} {data_type} {year}/{month:02d}")
            return ""

        key = self._make_key(data_type, symbol, year, month)
        client = self._get_client()

        # Merge with existing partition if it exists
        existing = self._try_read_parquet(key)
        if existing is not None and not existing.empty:
            combined = pd.concat([existing, df], ignore_index=True)
            # Dedup on first column (timestamp or date)
            dedup_col = combined.columns[0]
            combined = combined.drop_duplicates(subset=[dedup_col, "symbol"] if "symbol" in combined.columns else [dedup_col])
            combined = combined.sort_values(dedup_col).reset_index(drop=True)
        else:
            combined = df

        buf = io.BytesIO()
        combined.to_parquet(buf, index=False, compression="snappy")
        buf.seek(0)
        size = buf.getbuffer().nbytes

        client.put_object(
            self._bucket,
            key,
            buf,
            length=size,
            content_type="application/octet-stream",
        )
        logger.info(f"Wrote {len(combined)} rows to MinIO: {key} ({size:,} bytes)")
        return key

    def write_df_partitioned(
        self,
        df: pd.DataFrame,
        data_type: DataType,
        symbol: str,
        date_column: str = "timestamp",
    ) -> list[str]:
        """
        Write a DataFrame to MinIO, automatically partitioned by year/month.

        Args:
            df: DataFrame with a date/timestamp column.
            data_type: Data type key.
            symbol: Ticker symbol.
            date_column: Column name containing timestamps/dates for partitioning.

        Returns:
            List of object keys written.
        """
        if df.empty:
            return []

        df = df.copy()
        dates = pd.to_datetime(df[date_column], utc=True)
        df["_year"] = dates.dt.year
        df["_month"] = dates.dt.month

        keys = []
        for (year, month), group in df.groupby(["_year", "_month"]):
            group = group.drop(columns=["_year", "_month"])
            key = self.write_parquet(group, data_type, symbol, int(year), int(month))
            if key:
                keys.append(key)

        return keys

    # ── Read ───────────────────────────────────────────────────────────────

    def read_parquet(
        self,
        data_type: DataType,
        symbol: str,
        start: Optional[date] = None,
        end: Optional[date] = None,
    ) -> pd.DataFrame:
        """
        Read Parquet partitions for a symbol and optional date range.

        Downloads only the partitions that overlap with the requested range.

        Args:
            data_type: Data type to read.
            symbol: Ticker symbol.
            start: Optional start date (inclusive).
            end: Optional end date (inclusive).

        Returns:
            Merged DataFrame across all matching partitions, sorted by first column.
        """
        client = self._get_client()
        prefix = self._make_prefix(data_type, symbol)

        # List all objects under this symbol prefix
        try:
            objects = list(client.list_objects(self._bucket, prefix=prefix, recursive=True))
        except Exception as e:
            logger.warning(f"MinIO list_objects failed for {prefix}: {e}")
            return pd.DataFrame()

        if not objects:
            return pd.DataFrame()

        # Filter objects to year/month range
        if start or end:
            objects = [o for o in objects if self._key_in_range(o.object_name, start, end)]

        if not objects:
            return pd.DataFrame()

        dfs = []
        for obj in objects:
            df_part = self._try_read_parquet(obj.object_name)
            if df_part is not None and not df_part.empty:
                dfs.append(df_part)

        if not dfs:
            return pd.DataFrame()

        combined = pd.concat(dfs, ignore_index=True)

        # Filter to exact date range if provided
        if start or end:
            date_col = combined.columns[0]
            ts = pd.to_datetime(combined[date_col], utc=True)
            if start:
                combined = combined[ts.dt.date >= start]
            if end:
                combined = combined[ts.dt.date <= end]

        return combined.sort_values(combined.columns[0]).reset_index(drop=True)

    def _try_read_parquet(self, key: str) -> Optional[pd.DataFrame]:
        """Download and parse a Parquet object. Returns None on any error."""
        client = self._get_client()
        try:
            response = client.get_object(self._bucket, key)
            buf = io.BytesIO(response.read())
            return pd.read_parquet(buf)
        except Exception as e:
            # Object may not exist — that's normal
            if "NoSuchKey" in str(e) or "does not exist" in str(e).lower():
                return None
            logger.warning(f"MinIO read failed for {key}: {e}")
            return None
        finally:
            try:
                response.close()
                response.release_conn()
            except Exception:
                pass

    # ── Key Helpers ────────────────────────────────────────────────────────

    def _key_in_range(
        self,
        key: str,
        start: Optional[date],
        end: Optional[date],
    ) -> bool:
        """
        Return True if the object key's year/month partition overlaps with [start, end].

        Key format: {data_type}/{symbol}/{year}/{month:02d}/data.parquet
        """
        try:
            parts = key.split("/")
            year = int(parts[-3])
            month = int(parts[-2])
            # Partition covers entire month
            from datetime import date as date_type
            partition_start = date_type(year, month, 1)
            import calendar
            last_day = calendar.monthrange(year, month)[1]
            partition_end = date_type(year, month, last_day)

            if start and partition_end < start:
                return False
            if end and partition_start > end:
                return False
            return True
        except (IndexError, ValueError):
            return True  # If we can't parse, include it

    # ── Health Check ──────────────────────────────────────────────────────

    def ping(self) -> bool:
        """Verify MinIO connectivity by listing buckets."""
        try:
            client = self._get_client()
            client.list_buckets()
            return True
        except Exception as e:
            logger.warning(f"MinIO ping failed: {e}")
            return False

    def bucket_size_bytes(self) -> int:
        """Return total size of all objects in the bucket."""
        try:
            client = self._get_client()
            total = sum(
                obj.size
                for obj in client.list_objects(self._bucket, recursive=True)
            )
            return total
        except Exception as e:
            logger.warning(f"MinIO bucket_size failed: {e}")
            return 0
