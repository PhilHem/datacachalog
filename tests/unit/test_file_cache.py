"""Unit tests for FileCache adapter."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from datacachalog.core.models import CacheMetadata


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestGet:
    """Tests for get() method."""

    def test_get_nonexistent_key_returns_none(self, tmp_path: Path) -> None:
        """get() should return None for keys not in cache."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        assert cache.get("missing") is None

    def test_get_returns_none_when_metadata_missing(self, tmp_path: Path) -> None:
        """get() should return None when file exists but metadata is missing."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        # Create orphan file without metadata sidecar
        (tmp_path / "orphan").write_text("data")
        assert cache.get("orphan") is None


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestPut:
    """Tests for put() method."""

    def test_put_then_get_returns_path_and_metadata(self, tmp_path: Path) -> None:
        """put() should store file and metadata, get() should retrieve them."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        meta = CacheMetadata(etag='"abc123"', last_modified=datetime.now(UTC))
        cache.put("mykey", source, meta)

        result = cache.get("mykey")
        assert result is not None
        path, retrieved_meta = result
        assert path.read_text() == "data"
        assert retrieved_meta.etag == '"abc123"'

    def test_put_preserves_all_metadata_fields(self, tmp_path: Path) -> None:
        """put() should preserve all CacheMetadata fields in the sidecar."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        now = datetime.now(UTC)
        meta = CacheMetadata(
            etag='"abc"',
            last_modified=now,
            cached_at=now,
            source="s3://bucket/file.txt",
        )
        cache.put("key", source, meta)

        result = cache.get("key")
        assert result is not None
        _, retrieved = result
        assert retrieved.etag == '"abc"'
        assert retrieved.last_modified == now
        assert retrieved.source == "s3://bucket/file.txt"

    def test_put_creates_cache_directory(self, tmp_path: Path) -> None:
        """put() should create cache directory if it doesn't exist."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path / "nested" / "cache")
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("mykey", source, CacheMetadata(etag='"x"'))

        assert cache.get("mykey") is not None


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestInvalidate:
    """Tests for invalidate() method."""

    def test_invalidate_removes_cached_file_and_metadata(self, tmp_path: Path) -> None:
        """invalidate() should remove both cached file and metadata sidecar."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")
        cache.put("mykey", source, CacheMetadata(etag='"x"'))

        cache.invalidate("mykey")

        assert cache.get("mykey") is None
        assert not (tmp_path / "mykey").exists()
        assert not (tmp_path / "mykey.meta.json").exists()

    def test_invalidate_nonexistent_key_does_not_raise(self, tmp_path: Path) -> None:
        """invalidate() should not raise for keys not in cache."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        cache.invalidate("never_existed")  # Should not raise


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestInvalidatePrefix:
    """Tests for invalidate_prefix() method."""

    def test_invalidate_prefix_removes_matching_files(self, tmp_path: Path) -> None:
        """invalidate_prefix() should remove all files with matching prefix."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        # Create multiple cache entries under same prefix
        cache.put("logs/2024-01.parquet", source, CacheMetadata(etag='"a"'))
        cache.put("logs/2024-02.parquet", source, CacheMetadata(etag='"b"'))
        cache.put("logs/2024-03.parquet", source, CacheMetadata(etag='"c"'))

        cache.invalidate_prefix("logs")

        assert cache.get("logs/2024-01.parquet") is None
        assert cache.get("logs/2024-02.parquet") is None
        assert cache.get("logs/2024-03.parquet") is None

    def test_invalidate_prefix_returns_count(self, tmp_path: Path) -> None:
        """invalidate_prefix() should return count of deleted entries."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("logs/a.txt", source, CacheMetadata(etag='"a"'))
        cache.put("logs/b.txt", source, CacheMetadata(etag='"b"'))

        count = cache.invalidate_prefix("logs")

        assert count == 2

    def test_invalidate_prefix_preserves_other_files(self, tmp_path: Path) -> None:
        """invalidate_prefix() should not remove files with different prefix."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("logs/file.txt", source, CacheMetadata(etag='"a"'))
        cache.put("other/file.txt", source, CacheMetadata(etag='"b"'))

        cache.invalidate_prefix("logs")

        assert cache.get("logs/file.txt") is None
        assert cache.get("other/file.txt") is not None

    def test_invalidate_prefix_handles_no_matches(self, tmp_path: Path) -> None:
        """invalidate_prefix() should return 0 when no files match."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)

        count = cache.invalidate_prefix("nonexistent")

        assert count == 0

    def test_invalidate_prefix_handles_nested_directories(self, tmp_path: Path) -> None:
        """invalidate_prefix() should remove files in nested directories."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("logs/2024/01/data.parquet", source, CacheMetadata(etag='"a"'))
        cache.put("logs/2024/02/data.parquet", source, CacheMetadata(etag='"b"'))

        count = cache.invalidate_prefix("logs")

        assert count == 2
        assert cache.get("logs/2024/01/data.parquet") is None
        assert cache.get("logs/2024/02/data.parquet") is None


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestProtocolConformance:
    """Tests for CachePort protocol conformance."""

    def test_file_cache_satisfies_cache_port_protocol(self, tmp_path: Path) -> None:
        """FileCache should satisfy CachePort protocol."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.ports import CachePort

        cache = FileCache(cache_dir=tmp_path)
        assert isinstance(cache, CachePort)


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestCorruptMetadata:
    """Tests for handling corrupt cache metadata."""

    def test_get_raises_cache_corrupt_for_invalid_json(self, tmp_path: Path) -> None:
        """get() should raise CacheCorruptError when metadata is invalid JSON."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.exceptions import CacheCorruptError

        cache = FileCache(cache_dir=tmp_path)

        # Create file and corrupt metadata
        (tmp_path / "mykey").write_text("data")
        (tmp_path / "mykey.meta.json").write_text("not valid json{{{")

        with pytest.raises(CacheCorruptError) as exc_info:
            cache.get("mykey")

        assert exc_info.value.key == "mykey"
        assert exc_info.value.recovery_hint is not None


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestCacheSize:
    """Tests for cache size calculation."""

    def test_cache_size_calculation(self, tmp_path: Path) -> None:
        """FileCache can calculate total cache size."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source1 = tmp_path / "source1.txt"
        source1.write_text("data1")
        source2 = tmp_path / "source2.txt"
        source2.write_text("data2")

        cache.put("key1", source1, CacheMetadata(etag='"a"'))
        cache.put("key2", source2, CacheMetadata(etag='"b"'))

        size = cache.size()
        assert size > 0
        # Should include both data files and metadata files
        assert size >= len("data1") + len("data2")

    def test_cache_size_per_dataset(self, tmp_path: Path) -> None:
        """Catalog can calculate cache size per dataset."""
        from datacachalog import Dataset
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(source_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch to populate cache
        catalog.fetch("customers")

        size = catalog.cache_size("customers")
        assert size > 0

    def test_cache_size_includes_metadata_files(self, tmp_path: Path) -> None:
        """Cache size includes both data files and .meta.json files."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("test data")

        cache.put("mykey", source, CacheMetadata(etag='"abc"'))

        size = cache.size()
        # Should include both the data file and metadata file
        data_size = len("test data")
        # Metadata file has JSON content, so total should be > data_size
        assert size > data_size

    def test_cache_statistics_shows_total_size(self, tmp_path: Path) -> None:
        """Cache statistics method returns total size and file count."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source1 = tmp_path / "source1.txt"
        source1.write_text("data1")
        source2 = tmp_path / "source2.txt"
        source2.write_text("data2")

        cache.put("key1", source1, CacheMetadata(etag='"a"'))
        cache.put("key2", source2, CacheMetadata(etag='"b"'))

        stats = cache.statistics()
        assert stats["total_size"] > 0
        assert stats["file_count"] >= 2  # At least 2 data files (plus metadata files)


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestListAllKeys:
    """Tests for list_all_keys() method."""

    def test_list_all_keys_returns_empty_list_when_cache_empty(
        self, tmp_path: Path
    ) -> None:
        """list_all_keys() should return empty list when cache_dir is empty or doesn't exist."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        keys = cache.list_all_keys()
        assert keys == []

    def test_list_all_keys_returns_all_cache_keys(self, tmp_path: Path) -> None:
        """list_all_keys() should return all keys from .meta.json files in cache."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("key1", source, CacheMetadata(etag='"a"'))
        cache.put("key2", source, CacheMetadata(etag='"b"'))
        cache.put("key3", source, CacheMetadata(etag='"c"'))

        keys = cache.list_all_keys()
        assert len(keys) == 3
        assert "key1" in keys
        assert "key2" in keys
        assert "key3" in keys

    def test_list_all_keys_handles_nested_directories(self, tmp_path: Path) -> None:
        """list_all_keys() should return keys from nested directory structures."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("logs/2024/01/data.parquet", source, CacheMetadata(etag='"a"'))
        cache.put("logs/2024/02/data.parquet", source, CacheMetadata(etag='"b"'))
        cache.put("other/file.txt", source, CacheMetadata(etag='"c"'))

        keys = cache.list_all_keys()
        assert len(keys) == 3
        assert "logs/2024/01/data.parquet" in keys
        assert "logs/2024/02/data.parquet" in keys
        assert "other/file.txt" in keys

    def test_list_all_keys_excludes_orphaned_files(self, tmp_path: Path) -> None:
        """list_all_keys() should only return keys that have both data file and metadata file."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        # Create valid cache entry
        cache.put("valid_key", source, CacheMetadata(etag='"a"'))

        # Create orphaned data file without metadata
        (tmp_path / "orphan_data").write_text("orphan")

        # Create orphaned metadata without data file
        (tmp_path / "orphan_meta.meta.json").write_text(
            '{"etag": "b", "cached_at": "2024-01-01T00:00:00"}'
        )

        keys = cache.list_all_keys()
        assert len(keys) == 1
        assert "valid_key" in keys
        assert "orphan_data" not in keys
        assert "orphan_meta" not in keys

    def test_list_all_keys_returns_keys_relative_to_cache_dir(
        self, tmp_path: Path
    ) -> None:
        """list_all_keys() should return keys as relative paths from cache_dir root."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        cache.put("top_level.txt", source, CacheMetadata(etag='"a"'))
        cache.put("nested/path/file.txt", source, CacheMetadata(etag='"b"'))

        keys = cache.list_all_keys()
        assert "top_level.txt" in keys
        assert "nested/path/file.txt" in keys
        # Keys should be relative, not absolute paths
        assert not any("/" in key for key in keys if key == "top_level.txt")
        assert "nested/path/file.txt" in keys


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestInvalidCacheKeyError:
    """Tests for InvalidCacheKeyError exception."""

    def test_invalid_cache_key_error_inherits_from_cache_error(self) -> None:
        """InvalidCacheKeyError should inherit from CacheError."""
        from datacachalog.core.exceptions import CacheError, InvalidCacheKeyError

        error = InvalidCacheKeyError(key="bad/key", reason="contains ..")
        assert isinstance(error, CacheError)

    def test_invalid_cache_key_error_stores_key_and_reason(self) -> None:
        """InvalidCacheKeyError should store key and reason attributes."""
        from datacachalog.core.exceptions import InvalidCacheKeyError

        error = InvalidCacheKeyError(key="bad/key", reason="contains ..")
        assert error.key == "bad/key"
        assert error.reason == "contains .."

    def test_invalid_cache_key_error_has_recovery_hint(self) -> None:
        """InvalidCacheKeyError should provide a recovery_hint."""
        from datacachalog.core.exceptions import InvalidCacheKeyError

        error = InvalidCacheKeyError(key="bad/key", reason="contains ..")
        assert error.recovery_hint is not None
        assert isinstance(error.recovery_hint, str)
        assert len(error.recovery_hint) > 0


@pytest.mark.cache
@pytest.mark.tra("Adapter.FileCache")
@pytest.mark.tier(1)
class TestPathTraversal:
    """Tests for path traversal protection in FileCache."""

    def test_get_rejects_path_traversal(self, tmp_path: Path) -> None:
        """get() should reject keys with .. that would escape cache_dir."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.exceptions import InvalidCacheKeyError

        cache = FileCache(cache_dir=tmp_path)

        with pytest.raises(InvalidCacheKeyError):
            cache.get("../etc/passwd")

    def test_put_rejects_path_traversal(self, tmp_path: Path) -> None:
        """put() should reject keys with .. that would escape cache_dir."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.exceptions import InvalidCacheKeyError

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        with pytest.raises(InvalidCacheKeyError):
            cache.put("../etc/passwd", source, CacheMetadata(etag='"x"'))

    def test_invalidate_rejects_path_traversal(self, tmp_path: Path) -> None:
        """invalidate() should reject keys with .. that would escape cache_dir."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.exceptions import InvalidCacheKeyError

        cache = FileCache(cache_dir=tmp_path)

        with pytest.raises(InvalidCacheKeyError):
            cache.invalidate("../etc/passwd")

    def test_invalidate_prefix_rejects_path_traversal(self, tmp_path: Path) -> None:
        """invalidate_prefix() should reject prefixes with .. that would escape cache_dir."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.exceptions import InvalidCacheKeyError

        cache = FileCache(cache_dir=tmp_path)

        with pytest.raises(InvalidCacheKeyError):
            cache.invalidate_prefix("../etc")

    def test_file_path_rejects_absolute_key(self, tmp_path: Path) -> None:
        """invalidate() should reject absolute paths starting with /."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.exceptions import InvalidCacheKeyError

        cache = FileCache(cache_dir=tmp_path)

        with pytest.raises(InvalidCacheKeyError):
            cache.invalidate("/etc/passwd")

    def test_valid_nested_key_accepted(self, tmp_path: Path) -> None:
        """invalidate() should accept valid nested keys like 'logs/2024/file.txt'."""
        from datacachalog.adapters.cache import FileCache

        cache = FileCache(cache_dir=tmp_path)
        source = tmp_path / "source.txt"
        source.write_text("data")

        # First put a file with a nested key
        from datacachalog.core.models import CacheMetadata

        cache.put("logs/2024/file.txt", source, CacheMetadata(etag='"x"'))

        # Then invalidate it (should not raise)
        cache.invalidate("logs/2024/file.txt")

        # Verify it's gone
        assert cache.get("logs/2024/file.txt") is None
