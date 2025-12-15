"""Unit tests for FileCache adapter."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from datacachalog.core.models import CacheMetadata


@pytest.mark.cache
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
class TestProtocolConformance:
    """Tests for CachePort protocol conformance."""

    def test_file_cache_satisfies_cache_port_protocol(self, tmp_path: Path) -> None:
        """FileCache should satisfy CachePort protocol."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.core.ports import CachePort

        cache = FileCache(cache_dir=tmp_path)
        assert isinstance(cache, CachePort)


@pytest.mark.cache
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
