"""Unit tests for Catalog cache operations."""

from pathlib import Path

import pytest

from datacachalog import Dataset


@pytest.mark.core
@pytest.mark.tra("UseCase.IsStale")
@pytest.mark.tier(1)
class TestIsStale:
    """Tests for is_stale() method."""

    def test_is_stale_returns_true_when_not_cached(self, tmp_path: Path) -> None:
        """is_stale() should return True when dataset not in cache."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file but don't fetch
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        assert catalog.is_stale("customers") is True

    def test_is_stale_returns_false_when_fresh(self, tmp_path: Path) -> None:
        """is_stale() should return False when cache matches remote."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch to populate cache
        catalog.fetch("customers")

        # Assert: remote unchanged = fresh
        assert catalog.is_stale("customers") is False

    def test_is_stale_returns_true_when_remote_changed(self, tmp_path: Path) -> None:
        """is_stale() should return True when remote has changed."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch to populate cache
        catalog.fetch("customers")

        # Modify remote (changes ETag)
        remote_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Assert: remote changed = stale
        assert catalog.is_stale("customers") is True


@pytest.mark.core
@pytest.mark.tra("UseCase.Invalidate")
@pytest.mark.tier(1)
class TestInvalidate:
    """Tests for invalidate() method."""

    def test_invalidate_removes_from_cache(self, tmp_path: Path) -> None:
        """invalidate() should remove dataset from cache."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch to populate cache
        catalog.fetch("customers")
        assert catalog.is_stale("customers") is False

        # Invalidate
        catalog.invalidate("customers")

        # Assert: now stale (not in cache)
        assert catalog.is_stale("customers") is True

    def test_invalidate_causes_redownload_on_next_fetch(self, tmp_path: Path) -> None:
        """invalidate() should cause next fetch to re-download."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch and modify cached file
        result = catalog.fetch("customers")
        assert isinstance(result, Path)  # Type narrowing
        path = result
        path.write_text("MODIFIED")

        # Invalidate
        catalog.invalidate("customers")

        # Fetch again - should re-download
        result2 = catalog.fetch("customers")
        assert isinstance(result2, Path)  # Type narrowing
        path2 = result2

        assert path2.read_text() == "id,name\n1,Alice\n"


@pytest.mark.core
@pytest.mark.tra("UseCase.Invalidate")
@pytest.mark.tier(1)
class TestInvalidateGlob:
    """Tests for invalidate_glob() method."""

    def test_invalidate_glob_clears_all_cached_files(self, tmp_path: Path) -> None:
        """invalidate_glob() should remove all cached files for a glob dataset."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create multiple files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "2024-01.parquet").write_text("jan")
        (storage_dir / "2024-02.parquet").write_text("feb")
        (storage_dir / "2024-03.parquet").write_text("mar")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="monthly_data",
            source=str(storage_dir / "*.parquet"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Fetch to populate cache
        catalog.fetch("monthly_data")

        # Invalidate
        catalog.invalidate_glob("monthly_data")

        # Assert: all cache entries cleared (cache.get returns None)
        assert cache.get("monthly_data/2024-01.parquet") is None
        assert cache.get("monthly_data/2024-02.parquet") is None
        assert cache.get("monthly_data/2024-03.parquet") is None

    def test_invalidate_glob_returns_count(self, tmp_path: Path) -> None:
        """invalidate_glob() should return count of deleted entries."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.txt").write_text("a")
        (storage_dir / "b.txt").write_text("b")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="files", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        catalog.fetch("files")

        # Act
        count = catalog.invalidate_glob("files")

        # Assert
        assert count == 2

    def test_invalidate_glob_forces_redownload(self, tmp_path: Path) -> None:
        """invalidate_glob() should force re-download on next fetch."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.txt").write_text("original")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Fetch and modify cached file
        result = catalog.fetch("data")
        assert isinstance(result, list)  # Type narrowing for glob
        paths = result
        paths[0].write_text("MODIFIED")

        # Invalidate
        catalog.invalidate_glob("data")

        # Update source file
        (storage_dir / "data.txt").write_text("updated")

        # Fetch again - should get updated content
        result2 = catalog.fetch("data")
        assert isinstance(result2, list)  # Type narrowing for glob
        paths2 = result2
        assert paths2[0].read_text() == "updated"

    def test_invalidate_glob_on_non_glob_dataset_raises(self, tmp_path: Path) -> None:
        """invalidate_glob() should raise ValueError for non-glob datasets."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.txt").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Non-glob dataset (no wildcards)
        dataset = Dataset(
            name="single_file",
            source=str(storage_dir / "data.txt"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(ValueError, match="not a glob pattern"):
            catalog.invalidate_glob("single_file")


@pytest.mark.core
@pytest.mark.tra("UseCase.CleanOrphaned")
@pytest.mark.tier(1)
class TestCleanOrphaned:
    """Tests for clean_orphaned() method."""

    def test_clean_orphaned_returns_zero_when_cache_empty(self, tmp_path: Path) -> None:
        """clean_orphaned() should return 0 when cache is empty."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="customers", source=str(tmp_path / "data.csv"))
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        count = catalog.clean_orphaned()
        assert count == 0

    def test_clean_orphaned_returns_zero_when_no_orphaned_keys(
        self, tmp_path: Path
    ) -> None:
        """clean_orphaned() should return 0 when all cache keys are valid."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch to populate cache with valid key
        catalog.fetch("customers")

        count = catalog.clean_orphaned()
        assert count == 0

    def test_clean_orphaned_removes_orphaned_keys(self, tmp_path: Path) -> None:
        """clean_orphaned() should remove orphaned keys and return count."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Fetch to populate cache with valid key
        catalog.fetch("customers")

        # Manually add orphaned cache entry
        orphaned_file = cache_dir / "orphaned.csv"
        orphaned_file.write_text("orphaned data")
        orphaned_meta = cache_dir / "orphaned.csv.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        count = catalog.clean_orphaned()
        assert count == 1
        assert cache.get("orphaned") is None

    def test_clean_orphaned_preserves_glob_dataset_keys(self, tmp_path: Path) -> None:
        """clean_orphaned() should preserve hierarchical keys for glob datasets."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "2024-01.parquet").write_text("jan")
        (storage_dir / "2024-02.parquet").write_text("feb")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="monthly_data",
            source=str(storage_dir / "*.parquet"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Fetch to populate cache with glob keys
        catalog.fetch("monthly_data")

        # Add orphaned key
        orphaned_file = cache_dir / "orphaned.txt"
        orphaned_file.write_text("orphaned")
        orphaned_meta = cache_dir / "orphaned.txt.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        count = catalog.clean_orphaned()
        assert count == 1
        # Verify glob keys are preserved
        assert cache.get("monthly_data/2024-01.parquet") is not None
        assert cache.get("monthly_data/2024-02.parquet") is not None

    def test_clean_orphaned_preserves_versioned_keys(self, tmp_path: Path) -> None:
        """clean_orphaned() should preserve date-based versioned keys."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Manually create a versioned cache key (date-based format)
        cache_dir.mkdir(parents=True, exist_ok=True)
        versioned_key = "2024-01-15T120000.csv"
        versioned_file = cache_dir / versioned_key
        versioned_file.write_text("versioned data")
        versioned_meta = cache_dir / f"{versioned_key}.meta.json"
        versioned_meta.write_text(
            '{"etag": "v1", "cached_at": "2024-01-15T12:00:00", "source": "s3://bucket/data.csv"}'
        )

        # Add orphaned key
        orphaned_file = cache_dir / "orphaned.txt"
        orphaned_file.write_text("orphaned")
        orphaned_meta = cache_dir / "orphaned.txt.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        count = catalog.clean_orphaned()
        assert count == 1
        # Verify versioned key is preserved
        assert cache.get(versioned_key) is not None

    def test_clean_orphaned_handles_mixed_valid_and_orphaned(
        self, tmp_path: Path
    ) -> None:
        """clean_orphaned() should correctly identify and remove only orphaned keys."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file1 = storage_dir / "data1.csv"
        remote_file1.write_text("data1")
        remote_file2 = storage_dir / "data2.csv"
        remote_file2.write_text("data2")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset1 = Dataset(
            name="customers",
            source=str(remote_file1),
            cache_path=cache_dir / "customers.csv",
        )
        dataset2 = Dataset(
            name="products",
            source=str(remote_file2),
            cache_path=cache_dir / "products.csv",
        )
        catalog = Catalog(datasets=[dataset1, dataset2], storage=storage, cache=cache)

        # Fetch to populate cache with valid keys
        catalog.fetch("customers")
        catalog.fetch("products")

        # Add multiple orphaned keys
        for i in range(3):
            orphaned_file = cache_dir / f"orphaned{i}.txt"
            orphaned_file.write_text(f"orphaned{i}")
            orphaned_meta = cache_dir / f"orphaned{i}.txt.meta.json"
            orphaned_meta.write_text(
                f'{{"etag": "orphaned{i}", "cached_at": "2024-01-01T00:00:00", "source": ""}}'
            )

        count = catalog.clean_orphaned()
        assert count == 3
        # Verify valid keys are preserved
        assert cache.get("customers") is not None
        assert cache.get("products") is not None
        # Verify orphaned keys are removed
        assert cache.get("orphaned0") is None
        assert cache.get("orphaned1") is None
        assert cache.get("orphaned2") is None
