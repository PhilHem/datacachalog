"""Unit tests for Catalog service."""

from pathlib import Path

import pytest

from datacachalog import Dataset


@pytest.mark.core
class TestCatalogInit:
    """Tests for Catalog instantiation."""

    def test_catalog_accepts_datasets_storage_cache(self, tmp_path: Path) -> None:
        """Catalog should accept datasets, storage, and cache adapters."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        dataset = Dataset(name="test", source=str(tmp_path / "file.txt"))

        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        assert catalog is not None


@pytest.mark.core
class TestGetDataset:
    """Tests for dataset lookup."""

    def test_get_dataset_returns_dataset_by_name(self, tmp_path: Path) -> None:
        """get_dataset() should return the dataset with matching name."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        customers = Dataset(name="customers", source="/data/customers.csv")

        catalog = Catalog(datasets=[customers], storage=storage, cache=cache)

        assert catalog.get_dataset("customers") == customers

    def test_get_dataset_raises_key_error_for_unknown(self, tmp_path: Path) -> None:
        """get_dataset() should raise KeyError for unknown dataset names."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")

        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        with pytest.raises(KeyError):
            catalog.get_dataset("unknown")


@pytest.mark.core
class TestFetch:
    """Tests for fetch() method."""

    def test_fetch_downloads_on_cache_miss(self, tmp_path: Path) -> None:
        """fetch() should download file when cache is empty."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create a "remote" file
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

        # Act
        path = catalog.fetch("customers")

        # Assert
        assert path.exists()
        assert path.read_text() == "id,name\n1,Alice\n"

    def test_fetch_returns_cached_when_fresh(self, tmp_path: Path) -> None:
        """fetch() should return cached path when not stale."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file
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

        # First fetch populates cache
        path1 = catalog.fetch("customers")

        # Modify cached file to detect if re-downloaded
        path1.write_text("MODIFIED")

        # Second fetch (remote unchanged) should return cached
        path2 = catalog.fetch("customers")

        assert path2 == path1
        assert path2.read_text() == "MODIFIED"  # Proves no re-download

    def test_fetch_redownloads_when_stale(self, tmp_path: Path) -> None:
        """fetch() should re-download when remote has changed."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file
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

        # First fetch populates cache
        path1 = catalog.fetch("customers")
        original_content = path1.read_text()

        # Modify remote file (changes ETag)
        remote_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Second fetch should detect stale and re-download
        path2 = catalog.fetch("customers")

        assert path2.read_text() == "id,name\n1,Alice\n2,Bob\n"
        assert path2.read_text() != original_content

    def test_fetch_derives_cache_path_from_source(self, tmp_path: Path) -> None:
        """fetch() should derive cache path from source when not explicit."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.parquet"
        remote_file.write_text("parquet data")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Dataset without explicit cache_path
        dataset = Dataset(
            name="data",
            source=str(remote_file),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        path = catalog.fetch("data")

        # Should derive path from source filename
        assert path.exists()
        assert path.read_text() == "parquet data"

    def test_fetch_uses_explicit_cache_path(self, tmp_path: Path) -> None:
        """fetch() should use explicit cache_path when provided."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "source.csv"
        remote_file.write_text("data")

        cache_dir = tmp_path / "cache"
        custom_path = cache_dir / "custom" / "location.csv"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="test",
            source=str(remote_file),
            cache_path=custom_path,
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        catalog.fetch("test")

        # The download should go to the explicit cache_path location
        assert custom_path.exists()
        assert custom_path.read_text() == "data"


@pytest.mark.core
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

        # Assert: not cached = stale
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
        path = catalog.fetch("customers")
        path.write_text("MODIFIED")

        # Invalidate
        catalog.invalidate("customers")

        # Fetch again - should re-download
        path2 = catalog.fetch("customers")

        # Assert: content is fresh (not modified)
        assert path2.read_text() == "id,name\n1,Alice\n"


@pytest.mark.core
class TestDatasetsProperty:
    """Tests for the catalog.datasets property."""

    def test_datasets_returns_all_registered_datasets(self, tmp_path: Path) -> None:
        """datasets property should return all registered datasets."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Arrange
        ds1 = Dataset(name="alpha", source=str(tmp_path / "a.csv"))
        ds2 = Dataset(name="beta", source=str(tmp_path / "b.csv"))
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        catalog = Catalog(datasets=[ds1, ds2], storage=storage, cache=cache)

        # Act
        result = catalog.datasets

        # Assert
        assert len(result) == 2
        assert {d.name for d in result} == {"alpha", "beta"}

    def test_datasets_returns_empty_list_when_no_datasets(self, tmp_path: Path) -> None:
        """datasets property should return empty list when catalog has no datasets."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Arrange
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        # Act
        result = catalog.datasets

        # Assert
        assert result == []
