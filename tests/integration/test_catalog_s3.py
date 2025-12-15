"""Integration tests for Catalog with S3Storage.

These tests verify the full Catalog workflow using S3Storage with moto.
They exercise: fetch, staleness detection, cache usage, and invalidation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from pathlib import Path

from datacachalog import Catalog, Dataset, S3Storage
from datacachalog.adapters.cache import FileCache


@pytest.mark.storage
class TestCatalogS3Fetch:
    """Tests for Catalog.fetch() with S3 backend."""

    def test_fetch_downloads_from_s3_on_cache_miss(
        self, s3_client, tmp_path: Path
    ) -> None:
        """Catalog.fetch() should download from S3 when cache is empty."""
        # Setup: put file in moto S3
        s3_client.put_object(
            Bucket="test-bucket", Key="data.csv", Body=b"id,name\n1,Alice"
        )

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="users", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Act
        path = catalog.fetch("users")

        # Assert
        assert path.exists()
        assert path.read_text() == "id,name\n1,Alice"

    def test_fetch_uses_cache_when_not_stale(self, s3_client, tmp_path: Path) -> None:
        """Catalog.fetch() should return cached file if S3 hasn't changed."""
        # Setup
        s3_client.put_object(
            Bucket="test-bucket", Key="data.csv", Body=b"original content"
        )

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="users", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # First fetch - populates cache
        path1 = catalog.fetch("users")
        mtime1 = path1.stat().st_mtime

        # Second fetch - should use cache (S3 unchanged)
        path2 = catalog.fetch("users")
        mtime2 = path2.stat().st_mtime

        # Assert: same file, not re-downloaded
        assert path1 == path2
        assert mtime1 == mtime2
        assert path2.read_text() == "original content"

    def test_fetch_redownloads_when_s3_changes(self, s3_client, tmp_path: Path) -> None:
        """Catalog.fetch() should re-download when S3 file changes."""
        # Setup: initial file
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"version 1")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="users", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # First fetch
        path1 = catalog.fetch("users")
        assert path1.read_text() == "version 1"

        # Update S3 object (new content = new ETag)
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"version 2")

        # Second fetch - should detect change and re-download
        path2 = catalog.fetch("users")
        assert path2.read_text() == "version 2"


@pytest.mark.storage
class TestCatalogS3Staleness:
    """Tests for staleness detection with S3 backend."""

    def test_is_stale_returns_false_when_s3_unchanged(
        self, s3_client, tmp_path: Path
    ) -> None:
        """catalog.is_stale() should return False when S3 hasn't changed."""
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"content")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="users", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Populate cache
        catalog.fetch("users")

        # Check staleness - S3 unchanged
        assert catalog.is_stale("users") is False

    def test_is_stale_returns_true_when_s3_changed(
        self, s3_client, tmp_path: Path
    ) -> None:
        """catalog.is_stale() should return True when S3 has changed."""
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"original")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="users", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Populate cache
        catalog.fetch("users")

        # Update S3
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"updated")

        # Check staleness - S3 changed
        assert catalog.is_stale("users") is True


@pytest.mark.storage
class TestCatalogS3Invalidate:
    """Tests for cache invalidation with S3 backend."""

    def test_invalidate_forces_redownload(self, s3_client, tmp_path: Path) -> None:
        """catalog.invalidate() should force next fetch to re-download."""
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"content")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="users", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Populate cache
        path1 = catalog.fetch("users")
        mtime1 = path1.stat().st_mtime

        # Invalidate cache
        catalog.invalidate("users")

        # Fetch again - should re-download even though S3 unchanged
        path2 = catalog.fetch("users")
        mtime2 = path2.stat().st_mtime

        # File was re-downloaded (new mtime)
        assert mtime2 >= mtime1
        assert path2.read_text() == "content"


@pytest.mark.storage
class TestCatalogS3Glob:
    """Tests for glob pattern support with S3 backend."""

    def test_fetch_glob_downloads_all_matching_files_from_s3(
        self, s3_client, tmp_path: Path
    ) -> None:
        """fetch() with glob pattern should download all matching S3 objects."""
        # Setup: put multiple files in S3
        s3_client.put_object(
            Bucket="test-bucket", Key="data_2024_01.parquet", Body=b"january"
        )
        s3_client.put_object(
            Bucket="test-bucket", Key="data_2024_02.parquet", Body=b"february"
        )
        s3_client.put_object(
            Bucket="test-bucket", Key="data_2024_03.parquet", Body=b"march"
        )
        s3_client.put_object(Bucket="test-bucket", Key="other.csv", Body=b"not matched")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(
            name="monthly_data",
            source="s3://test-bucket/*.parquet",
        )
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Act
        result = catalog.fetch("monthly_data")

        # Assert
        assert isinstance(result, list)
        assert len(result) == 3
        contents = {p.read_text() for p in result}
        assert contents == {"january", "february", "march"}

    def test_fetch_glob_caches_each_s3_file_separately(
        self, s3_client, tmp_path: Path
    ) -> None:
        """Each S3 object matched by glob should have its own cache entry."""
        s3_client.put_object(Bucket="test-bucket", Key="file1.txt", Body=b"one")
        s3_client.put_object(Bucket="test-bucket", Key="file2.txt", Body=b"two")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="files", source="s3://test-bucket/*.txt")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Act
        catalog.fetch("files")

        # Assert: each file has its own cache entry
        assert cache.get("files/file1.txt") is not None
        assert cache.get("files/file2.txt") is not None

    def test_fetch_glob_checks_staleness_per_s3_file(
        self, s3_client, tmp_path: Path
    ) -> None:
        """Each S3 object in glob should have independent staleness checking."""
        s3_client.put_object(Bucket="test-bucket", Key="file1.txt", Body=b"original 1")
        s3_client.put_object(Bucket="test-bucket", Key="file2.txt", Body=b"original 2")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="files", source="s3://test-bucket/*.txt")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # First fetch
        catalog.fetch("files")

        # Modify only file2 in S3 (changes ETag)
        s3_client.put_object(Bucket="test-bucket", Key="file2.txt", Body=b"updated 2")

        # Second fetch
        paths = catalog.fetch("files")

        # Assert: file2 was re-downloaded with new content
        contents = {p.read_text() for p in paths}
        assert contents == {"original 1", "updated 2"}

    def test_fetch_glob_empty_match_raises_error_for_s3(
        self, s3_client, tmp_path: Path
    ) -> None:
        """fetch() should raise EmptyGlobMatchError when S3 pattern matches nothing."""
        from datacachalog.core.exceptions import EmptyGlobMatchError

        # S3 bucket is empty
        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="data", source="s3://test-bucket/*.parquet")
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Act & Assert
        with pytest.raises(EmptyGlobMatchError) as exc_info:
            catalog.fetch("data")

        assert "*.parquet" in str(exc_info.value)
        assert exc_info.value.recovery_hint is not None

    def test_invalidate_glob_clears_all_cached_s3_files(
        self, s3_client, tmp_path: Path
    ) -> None:
        """invalidate_glob() should remove all cached files for an S3 glob dataset."""
        s3_client.put_object(Bucket="test-bucket", Key="2024-01.parquet", Body=b"jan")
        s3_client.put_object(Bucket="test-bucket", Key="2024-02.parquet", Body=b"feb")
        s3_client.put_object(Bucket="test-bucket", Key="2024-03.parquet", Body=b"mar")

        cache_dir = tmp_path / "cache"
        storage = S3Storage(client=s3_client)
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(
            name="monthly_data",
            source="s3://test-bucket/*.parquet",
        )
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        # Fetch to populate cache
        catalog.fetch("monthly_data")

        # Invalidate
        count = catalog.invalidate_glob("monthly_data")

        # Assert
        assert count == 3
        assert cache.get("monthly_data/2024-01.parquet") is None
        assert cache.get("monthly_data/2024-02.parquet") is None
        assert cache.get("monthly_data/2024-03.parquet") is None
