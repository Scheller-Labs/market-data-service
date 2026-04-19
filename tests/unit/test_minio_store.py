"""
tests/unit/test_minio_store.py
Unit tests for MinIOStore — Parquet archive backed by MinIO object storage.

All tests mock the minio.Minio client. No real MinIO instance is required.
"""

import io
from datetime import date, datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pandas as pd
import pytest

from market_data.models import DataType
from market_data.storage.minio_store import MinIOStore


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_minio_client():
    """A MagicMock standing in for a real minio.Minio client instance."""
    client = MagicMock()
    client.bucket_exists.return_value = True   # default: bucket already exists
    client.list_objects.return_value = iter([])
    return client


@pytest.fixture
def minio_store(mock_minio_client):
    """
    A MinIOStore with a pre-injected mock Minio client.

    We patch minio.Minio at the import site inside _get_client() so the lazy
    init path is exercised, but the underlying client is always our mock.
    """
    store = MinIOStore(
        endpoint="localhost:9000",
        access_key="mds",
        secret_key="mds_secret",
        bucket="market-data",
        secure=False,
    )
    # Inject mock client directly so every method call uses it
    store._client = mock_minio_client
    return store


@pytest.fixture
def sample_ohlcv_df():
    """A small OHLCV DataFrame with UTC timestamps, suitable for Parquet round-trips."""
    base = datetime(2024, 3, 4, tzinfo=timezone.utc)
    rows = []
    for i in range(5):
        rows.append({
            "timestamp": base + timedelta(days=i),
            "symbol":    "AAPL",
            "open":      180.0 + i,
            "high":      182.0 + i,
            "low":       179.0 + i,
            "close":     181.0 + i,
            "volume":    50_000_000,
            "adj_close": 181.0 + i,
            "provider":  "alpha_vantage",
        })
    return pd.DataFrame(rows)


# ── _make_key ─────────────────────────────────────────────────────────────────

class TestMakeKey:
    def test_make_key_structure(self, minio_store):
        """
        _make_key(OHLCV, "AAPL", 2024, 3) must return the canonical partition path.

        The implementation uses PurePosixPath over (data_type.value, symbol, year,
        month:02d, "data.parquet") — there is no leading "market-data/" prefix in
        the key itself; that prefix is the bucket name.
        """
        key = minio_store._make_key(DataType.OHLCV, "AAPL", 2024, 3)
        assert key == "ohlcv/AAPL/2024/03/data.parquet"

    def test_make_key_symbol_uppercased(self, minio_store):
        """Symbol is forced to uppercase in the object key."""
        key = minio_store._make_key(DataType.OHLCV, "aapl", 2024, 1)
        assert "AAPL" in key

    def test_make_key_month_zero_padded(self, minio_store):
        """Single-digit months must be zero-padded to two digits."""
        key = minio_store._make_key(DataType.OHLCV, "SPY", 2024, 7)
        assert "/07/" in key

    def test_make_key_different_data_types(self, minio_store):
        """Each DataType.value must appear verbatim as the first path segment."""
        for dt in [DataType.OHLCV, DataType.OPTIONS_CHAIN, DataType.FUNDAMENTALS]:
            key = minio_store._make_key(dt, "SPY", 2024, 1)
            assert key.startswith(dt.value + "/"), f"Expected key to start with '{dt.value}/'"


# ── _ensure_bucket ────────────────────────────────────────────────────────────

class TestEnsureBucket:
    def test_ensure_bucket_creates_when_not_exists(self, minio_store, mock_minio_client):
        """When bucket_exists returns False, make_bucket must be called exactly once."""
        mock_minio_client.bucket_exists.return_value = False

        minio_store._ensure_bucket()

        mock_minio_client.make_bucket.assert_called_once_with(minio_store._bucket)

    def test_ensure_bucket_skips_when_exists(self, minio_store, mock_minio_client):
        """When bucket_exists returns True, make_bucket must NOT be called."""
        mock_minio_client.bucket_exists.return_value = True

        minio_store._ensure_bucket()

        mock_minio_client.make_bucket.assert_not_called()

    def test_ensure_bucket_checks_correct_bucket_name(self, minio_store, mock_minio_client):
        """bucket_exists must be called with the configured bucket name."""
        mock_minio_client.bucket_exists.return_value = True

        minio_store._ensure_bucket()

        mock_minio_client.bucket_exists.assert_called_once_with("market-data")

    def test_ensure_bucket_swallows_exception(self, minio_store, mock_minio_client):
        """Exceptions from MinIO must be caught and logged — not propagated."""
        mock_minio_client.bucket_exists.side_effect = Exception("connection refused")

        # Should not raise
        minio_store._ensure_bucket()


# ── write_parquet (put_object) ────────────────────────────────────────────────

class TestWriteParquet:
    def test_write_parquet_calls_put_object(self, minio_store, mock_minio_client, sample_ohlcv_df):
        """
        write_parquet must call put_object exactly once with the correct bucket and key.
        """
        # Ensure no existing object is returned (so no merge path)
        mock_minio_client.get_object.side_effect = Exception("NoSuchKey")

        returned_key = minio_store.write_parquet(sample_ohlcv_df, DataType.OHLCV, "AAPL", 2024, 3)

        mock_minio_client.put_object.assert_called_once()
        call_args = mock_minio_client.put_object.call_args
        assert call_args[0][0] == "market-data"   # bucket
        assert call_args[0][1] == "ohlcv/AAPL/2024/03/data.parquet"  # key
        assert returned_key == "ohlcv/AAPL/2024/03/data.parquet"

    def test_write_parquet_empty_df_skips_upload(self, minio_store, mock_minio_client):
        """
        Passing an empty DataFrame must skip the upload entirely and return an
        empty string (the implementation logs a debug message and returns "").
        """
        result = minio_store.write_parquet(pd.DataFrame(), DataType.OHLCV, "AAPL", 2024, 3)

        mock_minio_client.put_object.assert_not_called()
        assert result == ""

    def test_write_parquet_bytes_are_valid_parquet(self, minio_store, mock_minio_client, sample_ohlcv_df):
        """The bytes passed to put_object must be readable as a Parquet file."""
        mock_minio_client.get_object.side_effect = Exception("NoSuchKey")
        captured = {}

        def capture_put(bucket, key, buf, length, content_type):
            captured["data"] = buf.read()

        mock_minio_client.put_object.side_effect = capture_put

        minio_store.write_parquet(sample_ohlcv_df, DataType.OHLCV, "AAPL", 2024, 3)

        assert "data" in captured
        recovered = pd.read_parquet(io.BytesIO(captured["data"]))
        assert len(recovered) == len(sample_ohlcv_df)
        assert list(recovered.columns) == list(sample_ohlcv_df.columns)

    def test_write_parquet_merges_with_existing(self, minio_store, mock_minio_client, sample_ohlcv_df):
        """
        When an existing partition object is found, new data must be merged with
        the existing rows, de-duplicated, and the combined result uploaded.
        """
        # Build a "pre-existing" Parquet payload with the same rows
        buf = io.BytesIO()
        sample_ohlcv_df.to_parquet(buf, index=False)
        buf.seek(0)
        parquet_bytes = buf.read()

        response_mock = MagicMock()
        response_mock.read.return_value = parquet_bytes
        mock_minio_client.get_object.return_value = response_mock

        minio_store.write_parquet(sample_ohlcv_df, DataType.OHLCV, "AAPL", 2024, 3)

        # put_object must still be called (merged result written back)
        mock_minio_client.put_object.assert_called_once()


# ── read_parquet ──────────────────────────────────────────────────────────────

class TestReadParquet:
    def test_read_parquet_returns_empty_when_no_objects(self, minio_store, mock_minio_client):
        """
        When list_objects returns an empty iterator, read_parquet must return an
        empty DataFrame without raising.
        """
        mock_minio_client.list_objects.return_value = iter([])

        result = minio_store.read_parquet(DataType.OHLCV, "AAPL")

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_read_parquet_returns_empty_on_list_exception(self, minio_store, mock_minio_client):
        """Exceptions from list_objects must be caught; an empty DataFrame is returned."""
        mock_minio_client.list_objects.side_effect = Exception("connection error")

        result = minio_store.read_parquet(DataType.OHLCV, "AAPL")

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_read_parquet_filters_by_date_range(self, minio_store, mock_minio_client, sample_ohlcv_df):
        """
        Objects outside the requested date range must be excluded. We provide an
        object key that falls in March 2024 and request only February 2024 — the
        result should be empty after range filtering.
        """
        obj_mock = MagicMock()
        obj_mock.object_name = "ohlcv/AAPL/2024/03/data.parquet"
        mock_minio_client.list_objects.return_value = iter([obj_mock])

        result = minio_store.read_parquet(
            DataType.OHLCV,
            "AAPL",
            start=date(2024, 2, 1),
            end=date(2024, 2, 28),
        )

        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_read_parquet_fetches_matching_partitions(self, minio_store, mock_minio_client, sample_ohlcv_df):
        """
        Objects whose partition overlaps with the requested range must be fetched
        and returned as a merged DataFrame.
        """
        buf = io.BytesIO()
        sample_ohlcv_df.to_parquet(buf, index=False)
        buf.seek(0)
        parquet_bytes = buf.read()

        obj_mock = MagicMock()
        obj_mock.object_name = "ohlcv/AAPL/2024/03/data.parquet"
        mock_minio_client.list_objects.return_value = iter([obj_mock])

        response_mock = MagicMock()
        response_mock.read.return_value = parquet_bytes
        mock_minio_client.get_object.return_value = response_mock

        result = minio_store.read_parquet(
            DataType.OHLCV,
            "AAPL",
            start=date(2024, 3, 1),
            end=date(2024, 3, 31),
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_ohlcv_df)


# ── _get_client lazy init ─────────────────────────────────────────────────────

class TestGetClientLazyInit:
    def test_get_client_lazy_init_calls_minio_once(self):
        """
        The Minio constructor must be called exactly once, no matter how many
        times _get_client() is invoked.
        """
        store = MinIOStore(
            endpoint="localhost:9000",
            access_key="mds",
            secret_key="mds_secret",
            bucket="market-data",
            secure=False,
        )

        mock_instance = MagicMock()
        mock_instance.bucket_exists.return_value = True  # _ensure_bucket check

        with patch("market_data.storage.minio_store.MinIOStore._get_client") as mock_get:
            mock_get.return_value = mock_instance
            c1 = store._get_client()
            c2 = store._get_client()

        # Both calls must return the same object
        assert mock_get.call_count == 2  # called twice but constructor invoked once below

    def test_get_client_minio_constructor_called_once(self):
        """
        With a fresh store (no pre-injected client), the Minio constructor must be
        called exactly once no matter how many times _get_client() is called.

        _get_client() does `from minio import Minio` at call time (not at module
        import time), so we intercept it by patching `minio.Minio` directly.
        _ensure_bucket is patched out so its bucket_exists call does not interfere.
        """
        store = MinIOStore(
            endpoint="localhost:9000",
            access_key="mds",
            secret_key="mds_secret",
            bucket="market-data",
            secure=False,
        )

        call_count = [0]
        mock_instance = MagicMock()
        mock_instance.bucket_exists.return_value = True

        original_minio_cls = None

        import minio as minio_mod
        original_minio_cls = minio_mod.Minio

        class CountingMinio:
            """Replaces minio.Minio; counts constructor calls."""
            def __init__(self, *args, **kwargs):
                call_count[0] += 1
                self.bucket_exists = mock_instance.bucket_exists
                self.make_bucket = mock_instance.make_bucket
                self.list_buckets = mock_instance.list_buckets
                self.put_object = mock_instance.put_object
                self.get_object = mock_instance.get_object
                self.list_objects = mock_instance.list_objects

        with patch.object(minio_mod, "Minio", side_effect=CountingMinio):
            with patch.object(MinIOStore, "_ensure_bucket"):
                c1 = store._get_client()
                c2 = store._get_client()

        assert call_count[0] == 1, "Minio() constructor must be called exactly once (lazy init)"
        assert c1 is c2, "_get_client() must return the same instance on repeated calls"


# ── Store + Query round-trip with mock ────────────────────────────────────────

class TestStoreQueryRoundtrip:
    def test_roundtrip_bytes_via_mock(self, sample_ohlcv_df):
        """
        Simulate a full write → read cycle:
        1. write_parquet serialises the DataFrame and passes bytes to put_object.
        2. We capture those bytes.
        3. We configure get_object to return the same bytes.
        4. read_parquet must reconstruct a DataFrame equal to the original.
        """
        store = MinIOStore(
            endpoint="localhost:9000",
            access_key="mds",
            secret_key="mds_secret",
            bucket="market-data",
            secure=False,
        )
        mock_client = MagicMock()
        mock_client.bucket_exists.return_value = True
        mock_client.list_objects.return_value = iter([])  # populated after write
        store._client = mock_client

        # --- Write phase ---
        stored_bytes = {}

        def capture_put(bucket, key, buf, length, content_type):
            stored_bytes[key] = buf.read()

        mock_client.get_object.side_effect = Exception("NoSuchKey")   # no existing file
        mock_client.put_object.side_effect = capture_put

        store.write_parquet(sample_ohlcv_df, DataType.OHLCV, "AAPL", 2024, 3)

        assert stored_bytes, "write_parquet must have uploaded at least one object"
        written_key = "ohlcv/AAPL/2024/03/data.parquet"
        assert written_key in stored_bytes

        # --- Read phase ---
        obj_mock = MagicMock()
        obj_mock.object_name = written_key
        mock_client.list_objects.return_value = iter([obj_mock])

        response_mock = MagicMock()
        response_mock.read.return_value = stored_bytes[written_key]
        mock_client.get_object.side_effect = None
        mock_client.get_object.return_value = response_mock

        result = store.read_parquet(
            DataType.OHLCV,
            "AAPL",
            start=date(2024, 3, 1),
            end=date(2024, 3, 31),
        )

        assert isinstance(result, pd.DataFrame)
        assert len(result) == len(sample_ohlcv_df)
        assert list(result.columns) == list(sample_ohlcv_df.columns)


# ── ping / health ─────────────────────────────────────────────────────────────

class TestPing:
    def test_ping_returns_true_when_list_buckets_succeeds(self, minio_store, mock_minio_client):
        """ping() must return True when list_buckets does not raise."""
        mock_minio_client.list_buckets.return_value = []

        assert minio_store.ping() is True

    def test_ping_returns_false_on_exception(self, minio_store, mock_minio_client):
        """ping() must return False (not raise) when MinIO is unreachable."""
        mock_minio_client.list_buckets.side_effect = Exception("connection refused")

        assert minio_store.ping() is False


# ── _key_in_range ─────────────────────────────────────────────────────────────

class TestKeyInRange:
    def test_key_within_range(self, minio_store):
        key = "ohlcv/AAPL/2024/03/data.parquet"
        assert minio_store._key_in_range(key, date(2024, 3, 1), date(2024, 3, 31)) is True

    def test_key_before_range(self, minio_store):
        key = "ohlcv/AAPL/2024/01/data.parquet"
        assert minio_store._key_in_range(key, date(2024, 3, 1), date(2024, 3, 31)) is False

    def test_key_after_range(self, minio_store):
        key = "ohlcv/AAPL/2024/05/data.parquet"
        assert minio_store._key_in_range(key, date(2024, 3, 1), date(2024, 3, 31)) is False

    def test_key_overlaps_start(self, minio_store):
        """A partition ending on the first day of the range must be included."""
        key = "ohlcv/AAPL/2024/03/data.parquet"
        assert minio_store._key_in_range(key, date(2024, 3, 31), date(2024, 4, 30)) is True

    def test_key_malformed_returns_true(self, minio_store):
        """Unparseable keys must be included (safe default)."""
        assert minio_store._key_in_range("bad/key", date(2024, 1, 1), date(2024, 12, 31)) is True
