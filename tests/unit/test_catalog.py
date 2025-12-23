"""Unit tests for Catalog service."""

from pathlib import Path

import pytest

from datacachalog import Dataset


@pytest.mark.core
@pytest.mark.tra("UseCase.CatalogInit")
@pytest.mark.tier(1)
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
@pytest.mark.tra("UseCase.GetDataset")
@pytest.mark.tier(1)
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

    def test_get_dataset_raises_dataset_not_found_error(self, tmp_path: Path) -> None:
        """get_dataset() should raise DatasetNotFoundError for unknown names."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")

        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        with pytest.raises(DatasetNotFoundError, match="unknown"):
            catalog.get_dataset("unknown")

    def test_get_dataset_error_includes_available_datasets(
        self, tmp_path: Path
    ) -> None:
        """DatasetNotFoundError should list available dataset names."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        ds1 = Dataset(name="alpha", source="/a.csv")
        ds2 = Dataset(name="beta", source="/b.csv")

        catalog = Catalog(datasets=[ds1, ds2], storage=storage, cache=cache)

        with pytest.raises(DatasetNotFoundError) as exc_info:
            catalog.get_dataset("missing")

        assert "alpha" in exc_info.value.recovery_hint
        assert "beta" in exc_info.value.recovery_hint


@pytest.mark.core
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestFetchMissingCacheDir:
    """Tests for fetch() when cache configuration is missing."""

    def test_fetch_raises_configuration_error_without_cache_path_or_dir(
        self, tmp_path: Path
    ) -> None:
        """fetch() should raise ConfigurationError if neither cache_path nor cache_dir."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import ConfigurationError
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("data")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Dataset without explicit cache_path, catalog without cache_dir
        dataset = Dataset(name="customers", source=str(remote_file))
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)
        # Note: no cache_dir passed to Catalog

        with pytest.raises(ConfigurationError, match="cache"):
            catalog.fetch("customers")


@pytest.mark.core
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
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
        result = catalog.fetch("customers")
        assert isinstance(result, Path)  # Type narrowing
        path = result

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
        result1 = catalog.fetch("customers")
        assert isinstance(result1, Path)  # Type narrowing
        path1 = result1

        # Modify cached file to detect if re-downloaded
        path1.write_text("MODIFIED")

        # Second fetch (remote unchanged) should return cached
        result2 = catalog.fetch("customers")
        assert isinstance(result2, Path)  # Type narrowing
        path2 = result2

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
        result1 = catalog.fetch("customers")
        assert isinstance(result1, Path)  # Type narrowing
        path1 = result1
        original_content = path1.read_text()

        # Modify remote file (changes ETag)
        remote_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Second fetch should detect stale and re-download
        result2 = catalog.fetch("customers")
        assert isinstance(result2, Path)  # Type narrowing
        path2 = result2

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

        result = catalog.fetch("data")
        assert isinstance(result, Path)  # Type narrowing
        path = result

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

        # Assert: content is fresh (not modified)
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
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestFetchWithProgress:
    """Tests for fetch() with progress reporting."""

    def test_fetch_accepts_progress_parameter(self, tmp_path: Path) -> None:
        """fetch() should accept an optional progress parameter."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import NullProgressReporter
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

        # Act - should not raise
        reporter = NullProgressReporter()
        result = catalog.fetch("customers", progress=reporter)
        assert isinstance(result, Path)  # Type narrowing
        path = result

        # Assert
        assert path.exists()

    def test_fetch_calls_progress_reporter_on_download(self, tmp_path: Path) -> None:
        """fetch() should call progress reporter during download."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import ProgressCallback
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("x" * 1000)

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Track calls
        started_tasks: list[tuple[str, int]] = []
        finished_tasks: list[str] = []
        progress_calls: list[tuple[int, int]] = []

        class TrackingReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                started_tasks.append((name, total))

                def callback(downloaded: int, total: int) -> None:
                    progress_calls.append((downloaded, total))

                return callback

            def finish_task(self, name: str) -> None:
                finished_tasks.append(name)

        reporter = TrackingReporter()
        catalog.fetch("customers", progress=reporter)

        # Assert
        assert ("customers", 1000) in started_tasks
        assert "customers" in finished_tasks
        assert len(progress_calls) > 0
        assert progress_calls[-1][0] == 1000  # All bytes downloaded

    def test_fetch_does_not_call_progress_when_cache_hit(self, tmp_path: Path) -> None:
        """fetch() should not call progress reporter when returning from cache."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import ProgressCallback
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("content")

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
        catalog.fetch("customers")

        # Track second fetch
        started_tasks: list[str] = []

        class TrackingReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                started_tasks.append(name)
                return lambda d, t: None

            def finish_task(self, name: str) -> None:
                pass

        reporter = TrackingReporter()
        catalog.fetch("customers", progress=reporter)

        # Assert: no progress since cache was used
        assert started_tasks == []


@pytest.mark.core
@pytest.mark.tra("UseCase.FetchAll")
@pytest.mark.tier(1)
class TestFetchAll:
    """Tests for fetch_all() method."""

    def test_fetch_all_exists(self, tmp_path: Path) -> None:
        """Catalog should have fetch_all() method."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        assert hasattr(catalog, "fetch_all")
        assert callable(catalog.fetch_all)

    def test_fetch_all_returns_dict_of_paths(self, tmp_path: Path) -> None:
        """fetch_all() should return dict mapping names to paths."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("a content")
        (storage_dir / "b.csv").write_text("b content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        datasets = [
            Dataset(name="alpha", source=str(storage_dir / "a.csv")),
            Dataset(name="beta", source=str(storage_dir / "b.csv")),
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        result = catalog.fetch_all()

        # Assert
        assert isinstance(result, dict)
        assert set(result.keys()) == {"alpha", "beta"}
        alpha_path = result["alpha"]
        assert isinstance(alpha_path, Path)  # Type narrowing
        assert alpha_path.read_text() == "a content"
        beta_path = result["beta"]
        assert isinstance(beta_path, Path)  # Type narrowing
        assert beta_path.read_text() == "b content"

    def test_fetch_all_accepts_progress_parameter(self, tmp_path: Path) -> None:
        """fetch_all() should accept optional progress reporter."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import NullProgressReporter
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="alpha", source=str(storage_dir / "a.csv"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act - should not raise
        reporter = NullProgressReporter()
        result = catalog.fetch_all(progress=reporter)

        assert "alpha" in result

    def test_fetch_all_reports_progress_for_each_dataset(self, tmp_path: Path) -> None:
        """fetch_all() should report progress for each download."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import ProgressCallback
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("a" * 100)
        (storage_dir / "b.csv").write_text("b" * 200)

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        datasets = [
            Dataset(name="alpha", source=str(storage_dir / "a.csv")),
            Dataset(name="beta", source=str(storage_dir / "b.csv")),
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Track progress
        started_tasks: list[tuple[str, int]] = []
        finished_tasks: list[str] = []

        class TrackingReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                started_tasks.append((name, total))
                return lambda d, t: None

            def finish_task(self, name: str) -> None:
                finished_tasks.append(name)

        reporter = TrackingReporter()
        catalog.fetch_all(progress=reporter)

        # Assert both datasets reported
        assert ("alpha", 100) in started_tasks
        assert ("beta", 200) in started_tasks
        assert "alpha" in finished_tasks
        assert "beta" in finished_tasks

    def test_fetch_all_returns_empty_dict_when_no_datasets(
        self, tmp_path: Path
    ) -> None:
        """fetch_all() should return empty dict when catalog is empty."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage = FilesystemStorage()
        cache = FileCache(cache_dir=tmp_path / "cache")
        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        result = catalog.fetch_all()

        assert result == {}

    @pytest.mark.tra("Domain.Catalog")
    @pytest.mark.tier(1)
    def test_fetch_all_without_executor_uses_sequential_execution(
        self, tmp_path: Path
    ) -> None:
        """fetch_all() with executor=None should execute sequentially, not create ThreadPoolExecutor."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import ProgressCallback
        from datacachalog.core.services import Catalog

        # Setup multiple files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("a")
        (storage_dir / "b.csv").write_text("b")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        datasets = [
            Dataset(name="alpha", source=str(storage_dir / "a.csv")),
            Dataset(name="beta", source=str(storage_dir / "b.csv")),
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
            executor=None,  # Explicitly no executor
        )

        # Track execution order to verify sequential execution
        execution_order: list[str] = []

        class OrderTrackingReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                execution_order.append(name)
                return lambda d, t: None

            def finish_task(self, name: str) -> None:
                pass

        tracker = OrderTrackingReporter()
        result = catalog.fetch_all(progress=tracker, max_workers=None)

        # Verify results are correct
        assert len(result) == 2
        assert "alpha" in result
        assert "beta" in result
        # Verify sequential execution (no parallel ThreadPoolExecutor created)
        # When executor=None, should use SynchronousExecutor which executes sequentially
        assert len(execution_order) == 2
        assert execution_order == ["alpha", "beta"] or execution_order == [
            "beta",
            "alpha",
        ]

    @pytest.mark.tra("Domain.Catalog")
    @pytest.mark.tier(1)
    def test_fetch_all_without_executor_preserves_functionality(
        self, tmp_path: Path
    ) -> None:
        """fetch_all() should still return correct results when no executor provided."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("a content")
        (storage_dir / "b.csv").write_text("b content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        datasets = [
            Dataset(name="alpha", source=str(storage_dir / "a.csv")),
            Dataset(name="beta", source=str(storage_dir / "b.csv")),
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
            executor=None,  # Explicitly no executor
        )

        # Act
        result = catalog.fetch_all()

        # Assert - should work correctly even without executor
        assert isinstance(result, dict)
        assert set(result.keys()) == {"alpha", "beta"}
        alpha_path = result["alpha"]
        assert isinstance(alpha_path, Path)
        assert alpha_path.read_text() == "a content"
        beta_path = result["beta"]
        assert isinstance(beta_path, Path)
        assert beta_path.read_text() == "b content"


@pytest.mark.core
@pytest.mark.tra("UseCase.FetchAll")
@pytest.mark.tier(1)
class TestFetchAllParallel:
    """Tests for parallel fetch_all()."""

    def test_fetch_all_accepts_max_workers_parameter(self, tmp_path: Path) -> None:
        """fetch_all() should accept max_workers parameter."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="alpha", source=str(storage_dir / "a.csv"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act - should not raise
        result = catalog.fetch_all(max_workers=2)

        assert "alpha" in result

    def test_fetch_all_parallel_downloads_multiple_files(self, tmp_path: Path) -> None:
        """fetch_all(max_workers=N) should download N files concurrently."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import ProgressCallback
        from datacachalog.core.services import Catalog

        # Setup multiple files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        for i in range(4):
            (storage_dir / f"file{i}.csv").write_text(f"content {i}")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        datasets = [
            Dataset(name=f"ds{i}", source=str(storage_dir / f"file{i}.csv"))
            for i in range(4)
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Track tasks
        started_tasks: list[str] = []
        finished_tasks: list[str] = []

        class TrackingReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                started_tasks.append(name)
                return lambda d, t: None

            def finish_task(self, name: str) -> None:
                finished_tasks.append(name)

        tracker = TrackingReporter()
        result = catalog.fetch_all(progress=tracker, max_workers=2)

        assert len(result) == 4
        assert set(started_tasks) == {"ds0", "ds1", "ds2", "ds3"}
        assert set(finished_tasks) == {"ds0", "ds1", "ds2", "ds3"}

    def test_fetch_all_sequential_when_max_workers_1(self, tmp_path: Path) -> None:
        """fetch_all(max_workers=1) should download sequentially."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.ports import ProgressCallback
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("a")
        (storage_dir / "b.csv").write_text("b")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        datasets = [
            Dataset(name="alpha", source=str(storage_dir / "a.csv")),
            Dataset(name="beta", source=str(storage_dir / "b.csv")),
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Track order
        order: list[str] = []

        class OrderTracker:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                order.append(f"start:{name}")
                return lambda d, t: None

            def finish_task(self, name: str) -> None:
                order.append(f"finish:{name}")

        tracker = OrderTracker()
        catalog.fetch_all(progress=tracker, max_workers=1)

        # Sequential: start A, finish A, start B, finish B
        # (not interleaved)
        assert order[0].startswith("start:")
        assert order[1].startswith("finish:")
        assert order[2].startswith("start:")
        assert order[3].startswith("finish:")


@pytest.mark.core
@pytest.mark.tra("Domain.Catalog")
@pytest.mark.tier(1)
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


@pytest.mark.core
@pytest.mark.tra("UseCase.CatalogInit")
@pytest.mark.tier(1)
class TestCatalogFromDirectory:
    """Tests for Catalog.from_directory() factory method."""

    def test_from_directory_discovers_project_root(self, tmp_path: Path) -> None:
        """from_directory() should discover project root from marker files."""
        from datacachalog.core.services import Catalog

        # Create project structure with .git marker
        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        dataset = Dataset(name="test", source=str(source_file))
        catalog = Catalog.from_directory([dataset], directory=tmp_path)

        # Default cache_dir should be {root}/data
        assert catalog._cache_dir == tmp_path / "data"

    def test_from_directory_resolves_relative_cache_paths(self, tmp_path: Path) -> None:
        """from_directory() should resolve relative cache_path against root."""
        from datacachalog.core.services import Catalog

        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        dataset = Dataset(
            name="test",
            source=str(source_file),
            cache_path=Path("custom/cache.txt"),  # relative path
        )
        catalog = Catalog.from_directory([dataset], directory=tmp_path)

        resolved = catalog.get_dataset("test")
        assert resolved.cache_path == tmp_path / "custom/cache.txt"

    def test_from_directory_accepts_custom_cache_dir(self, tmp_path: Path) -> None:
        """from_directory() should accept custom cache directory."""
        from datacachalog.core.services import Catalog

        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        dataset = Dataset(name="test", source=str(source_file))
        catalog = Catalog.from_directory(
            [dataset],
            directory=tmp_path,
            cache_dir="custom_cache",
        )

        assert catalog._cache_dir == tmp_path / "custom_cache"

    def test_from_directory_accepts_absolute_cache_dir(self, tmp_path: Path) -> None:
        """from_directory() should accept absolute cache directory path."""
        from datacachalog.core.services import Catalog

        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")
        absolute_cache = tmp_path / "absolute_cache"

        dataset = Dataset(name="test", source=str(source_file))
        catalog = Catalog.from_directory(
            [dataset],
            directory=tmp_path,
            cache_dir=absolute_cache,
        )

        assert catalog._cache_dir == absolute_cache

    def test_from_directory_creates_working_catalog(self, tmp_path: Path) -> None:
        """from_directory() should create a fully functional catalog."""
        from datacachalog.core.services import Catalog

        # Create project structure
        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("test content")

        dataset = Dataset(
            name="test",
            source=str(source_file),
            cache_path=Path("data/test.txt"),
        )
        catalog = Catalog.from_directory([dataset], directory=tmp_path)

        # Should be able to fetch
        result = catalog.fetch("test")
        assert isinstance(result, Path)  # Type narrowing
        path = result
        assert path.exists()
        assert path.read_text() == "test content"

    def test_from_directory_uses_cwd_when_no_directory(self, tmp_path: Path) -> None:
        """from_directory() should use current directory when not specified."""
        import os

        from datacachalog.core.services import Catalog

        # Create project structure in tmp_path
        (tmp_path / ".git").mkdir()
        source_file = tmp_path / "source.txt"
        source_file.write_text("content")

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            dataset = Dataset(name="test", source=str(source_file))
            catalog = Catalog.from_directory([dataset])

            assert catalog._cache_dir == tmp_path / "data"
        finally:
            os.chdir(original_cwd)


@pytest.mark.core
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestFetchGlob:
    """Tests for glob pattern support in fetch()."""

    def test_fetch_glob_returns_list_of_paths(self, tmp_path: Path) -> None:
        """fetch() with glob pattern should return list[Path]."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create multiple files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data_2024_01.parquet").write_text("jan")
        (storage_dir / "data_2024_02.parquet").write_text("feb")
        (storage_dir / "data_2024_03.parquet").write_text("mar")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Dataset with glob pattern
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

        # Act
        result = catalog.fetch("monthly_data")

        # Assert
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(p, Path) for p in result)

    def test_fetch_glob_downloads_all_matching_files(self, tmp_path: Path) -> None:
        """fetch() should download all files matching the glob pattern."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.parquet").write_text("content a")
        (storage_dir / "b.parquet").write_text("content b")
        (storage_dir / "c.csv").write_text("not matched")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "*.parquet"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        result = catalog.fetch("data")
        assert isinstance(result, list)  # Type narrowing for glob
        paths = result

        # Assert: only .parquet files matched
        assert len(paths) == 2
        contents = {p.read_text() for p in paths}
        assert contents == {"content a", "content b"}

    def test_fetch_glob_caches_each_file_separately(self, tmp_path: Path) -> None:
        """Each file matched by glob should have its own cache entry."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "file1.txt").write_text("one")
        (storage_dir / "file2.txt").write_text("two")

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

        # Act
        catalog.fetch("files")

        # Assert: each file has its own cache entry
        assert cache.get("files/file1.txt") is not None
        assert cache.get("files/file2.txt") is not None

    def test_fetch_glob_empty_match_raises_error(self, tmp_path: Path) -> None:
        """fetch() should raise EmptyGlobMatchError when pattern matches nothing."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import EmptyGlobMatchError
        from datacachalog.core.services import Catalog

        # Setup: empty directory
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "*.parquet"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act & Assert
        with pytest.raises(EmptyGlobMatchError) as exc_info:
            catalog.fetch("data")

        assert "*.parquet" in str(exc_info.value)
        assert exc_info.value.recovery_hint is not None

    def test_fetch_non_glob_still_returns_single_path(self, tmp_path: Path) -> None:
        """fetch() without glob should return single Path (backward compatible)."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "single.csv").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="single",
            source=str(storage_dir / "single.csv"),
            cache_path=cache_dir / "single.csv",
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        # Act
        result = catalog.fetch("single")

        # Assert: single Path, not list
        assert isinstance(result, Path)
        assert result.read_text() == "content"

    def test_fetch_glob_checks_staleness_per_file(self, tmp_path: Path) -> None:
        """Each file in glob should have independent staleness checking."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        file1 = storage_dir / "file1.txt"
        file2 = storage_dir / "file2.txt"
        file1.write_text("original 1")
        file2.write_text("original 2")

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

        # First fetch
        catalog.fetch("files")

        # Modify only file2
        file2.write_text("updated 2")

        # Second fetch
        result = catalog.fetch("files")
        assert isinstance(result, list)  # Type narrowing for glob
        paths = result

        # Assert: file2 was re-downloaded with new content
        contents = {p.read_text() for p in paths}
        assert contents == {"original 1", "updated 2"}

    def test_fetch_with_dry_run_returns_cached_path_if_fresh(
        self, tmp_path: Path
    ) -> None:
        """fetch(dry_run=True) should return cached path when cache is fresh."""
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
        result1 = catalog.fetch("customers")
        assert isinstance(result1, Path)
        path1 = result1

        # Dry-run fetch should return cached path (fresh)
        result2 = catalog.fetch("customers", dry_run=True)
        assert isinstance(result2, Path)
        path2 = result2

        assert path2 == path1, "Dry-run should return cached path when fresh"

    def test_fetch_with_dry_run_checks_staleness_but_does_not_download(
        self, tmp_path: Path
    ) -> None:
        """fetch(dry_run=True) should check staleness but skip download and cache update."""
        import time

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
        result1 = catalog.fetch("customers")
        assert isinstance(result1, Path)
        path1 = result1
        original_content = path1.read_text()

        # Get cache metadata before modification
        cached_before = cache.get("customers")
        assert cached_before is not None

        # Modify remote file (changes ETag/mtime)
        time.sleep(0.1)  # Ensure mtime changes
        remote_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Dry-run fetch should check staleness but not download
        result2 = catalog.fetch("customers", dry_run=True)
        assert isinstance(result2, Path)
        path2 = result2

        # Cache should be unchanged (still has old content)
        assert path2.read_text() == original_content, (
            "Cache should not be updated in dry-run"
        )
        cached_after = cache.get("customers")
        assert cached_before == cached_after, (
            "Cache metadata should not change in dry-run"
        )

    def test_fetch_all_with_dry_run(self, tmp_path: Path) -> None:
        """fetch_all(dry_run=True) should check all datasets without downloading."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create "remote" files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "orders.csv").write_text("id,amount\n1,100\n")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        customers = Dataset(
            name="customers",
            source=str(storage_dir / "customers.csv"),
            cache_path=cache_dir / "customers.csv",
        )
        orders = Dataset(
            name="orders",
            source=str(storage_dir / "orders.csv"),
            cache_path=cache_dir / "orders.csv",
        )
        catalog = Catalog(datasets=[customers, orders], storage=storage, cache=cache)

        # First fetch_all populates cache
        catalog.fetch_all()

        # Get cache state before dry-run
        cached_customers_before = cache.get("customers")
        cached_orders_before = cache.get("orders")

        # Dry-run fetch_all
        results = catalog.fetch_all(dry_run=True)

        assert isinstance(results, dict)
        assert "customers" in results
        assert "orders" in results

        # Cache should be unchanged
        cached_customers_after = cache.get("customers")
        cached_orders_after = cache.get("orders")
        assert cached_customers_before == cached_customers_after
        assert cached_orders_before == cached_orders_after

    def test_fetch_glob_with_dry_run(self, tmp_path: Path) -> None:
        """fetch(dry_run=True) for glob dataset should check staleness without downloading."""
        import time

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        file1 = storage_dir / "file1.txt"
        file2 = storage_dir / "file2.txt"
        file1.write_text("original 1")
        file2.write_text("original 2")

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

        # First fetch populates cache
        catalog.fetch("files")

        # Get cache state before modification (check that entries exist)
        cache_entry1_before = cache.get("files/file1.txt")
        cache_entry2_before = cache.get("files/file2.txt")
        assert cache_entry1_before is not None
        assert cache_entry2_before is not None

        # Modify one file
        time.sleep(0.1)
        file2.write_text("updated 2")

        # Dry-run fetch
        result = catalog.fetch("files", dry_run=True)
        assert isinstance(result, list)

        # Cache should be unchanged (same metadata)
        cache_entry1_after = cache.get("files/file1.txt")
        cache_entry2_after = cache.get("files/file2.txt")
        assert cache_entry1_before == cache_entry1_after, (
            "Cache should not be modified in dry-run"
        )
        assert cache_entry2_before == cache_entry2_after, (
            "Cache should not be modified in dry-run"
        )

    @pytest.mark.property
    def test_fetch_dry_run_cache_immutability_property(self, tmp_path: Path) -> None:
        """Property: Multiple fetch(dry_run=True) calls never modify cache state."""
        import time
        import uuid

        from hypothesis import given
        from hypothesis.strategies import binary, integers

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        @given(
            content=binary(),
            num_calls=integers(min_value=1, max_value=10),
            has_cache=integers(min_value=0, max_value=2),  # 0=missing, 1=fresh, 2=stale
        )
        def _test_cache_immutability(
            content: bytes, num_calls: int, has_cache: int
        ) -> None:
            # Setup: create fresh directories for each hypothesis run
            run_id = uuid.uuid4().hex[:8]
            storage_dir = tmp_path / f"storage_{run_id}"
            storage_dir.mkdir()
            remote_file = storage_dir / "data.bin"
            remote_file.write_bytes(content)

            cache_dir = tmp_path / f"cache_{run_id}"
            storage = FilesystemStorage()
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(
                name="test_data",
                source=str(remote_file),
                cache_path=cache_dir / "test_data.bin",
            )
            catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

            # Setup cache state based on has_cache parameter
            if has_cache > 0:  # 1=fresh, 2=stale
                # Populate cache with initial fetch
                catalog.fetch("test_data")

                if has_cache == 2:  # stale cache
                    # Modify remote file to make cache stale
                    time.sleep(0.1)  # Ensure mtime changes
                    remote_file.write_bytes(content + b"_modified")

            # Capture initial cache state (may be None if no cache)
            cached_initial = cache.get("test_data")
            if cached_initial is not None:
                cached_path_initial, cached_meta_initial = cached_initial
                file_content_initial = cached_path_initial.read_bytes()
            else:
                cached_path_initial = None
                cached_meta_initial = None
                file_content_initial = None

            # Perform multiple dry-run calls
            for _ in range(num_calls):
                result = catalog.fetch("test_data", dry_run=True)
                assert isinstance(result, Path)

            # Verify cache state unchanged after all dry-run calls
            cached_after = cache.get("test_data")

            if cached_initial is None:
                # No cache before - should still be no cache after
                assert cached_after is None, "Cache should not be created in dry-run"
            else:
                # Cache existed before - verify unchanged
                assert cached_after is not None, (
                    "Cache should not be removed in dry-run"
                )
                cached_path_after, cached_meta_after = cached_after

                # Verify metadata unchanged
                assert cached_meta_initial == cached_meta_after, (
                    "Cache metadata should not change in dry-run"
                )

                # Verify file contents unchanged
                file_content_after = cached_path_after.read_bytes()
                assert file_content_initial == file_content_after, (
                    "Cache file contents should not change in dry-run"
                )

                # Verify path unchanged
                assert cached_path_initial == cached_path_after, (
                    "Cache path should not change in dry-run"
                )

        _test_cache_immutability()


@pytest.mark.core
@pytest.mark.tra("UseCase.Versions")
@pytest.mark.tier(1)
class TestVersions:
    """Tests for catalog.versions() method."""

    @pytest.mark.tier(2)
    def test_versions_returns_object_versions(self, tmp_path: Path) -> None:
        """versions() should return list of ObjectVersion for dataset."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.models import ObjectVersion
        from datacachalog.core.services import Catalog

        with mock_aws():
            # Setup versioned S3 bucket
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload multiple versions
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v1")
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v2")
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v3")

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Act
            versions = catalog.versions("data")

            # Assert
            assert isinstance(versions, list)
            assert len(versions) == 3
            assert all(isinstance(v, ObjectVersion) for v in versions)

    @pytest.mark.tier(2)
    def test_versions_respects_limit(self, tmp_path: Path) -> None:
        """versions(limit=N) should return at most N versions."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload 5 versions
            for i in range(5):
                client.put_object(
                    Bucket="versioned-bucket", Key="data.txt", Body=f"v{i}".encode()
                )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Act
            versions = catalog.versions("data", limit=3)

            # Assert
            assert len(versions) == 3

    @pytest.mark.tier(1)
    def test_versions_raises_dataset_not_found(self, tmp_path: Path) -> None:
        """versions() should raise DatasetNotFoundError for unknown dataset."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        with pytest.raises(DatasetNotFoundError, match="unknown"):
            catalog.versions("unknown")

    @pytest.mark.tier(1)
    def test_versions_raises_on_non_versioned_storage(self, tmp_path: Path) -> None:
        """versions() should raise VersioningNotSupportedError for filesystem."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import VersioningNotSupportedError
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.txt").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "data.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(VersioningNotSupportedError):
            catalog.versions("data")


@pytest.mark.core
@pytest.mark.tier(2)
class TestFetchVersion:
    """Tests for fetch() with version_id parameter."""

    def test_fetch_with_version_id_downloads_specific_version(
        self, tmp_path: Path
    ) -> None:
        """fetch(version_id=) should download that specific version."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload two versions
            resp1 = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"first version"
            )
            v1_id = resp1["VersionId"]
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"second version"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch the first version (not the latest)
            result = catalog.fetch("data", version_id=v1_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            assert path.exists()
            assert path.read_text() == "first version"

    def test_fetch_version_uses_version_aware_cache_key(self, tmp_path: Path) -> None:
        """Versioned fetches should cache under version-specific key."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )
            version_id = resp["VersionId"]

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch with version_id
            result = catalog.fetch("data", version_id=version_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # Cache key should be date-based (not {name}@{version_id})
            filename = path.name
            assert filename.endswith(".txt")
            # Should be date-based format: YYYY-MM-DDTHHMMSS.txt
            date_part = filename[:-4]
            assert len(date_part) == 17  # YYYY-MM-DDTHHMMSS
            assert date_part[10] == "T"  # Date-time separator
            # Verify it's cached
            assert cache.get(filename) is not None

    def test_fetch_version_caches_separately_from_latest(self, tmp_path: Path) -> None:
        """Versioned fetch and normal fetch should use separate cache entries."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp1 = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"old version"
            )
            v1_id = resp1["VersionId"]
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"new version"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch latest (normal)
            result_latest = catalog.fetch("data")
            assert isinstance(result_latest, Path)  # Type narrowing
            latest_path = result_latest

            # Fetch specific old version
            result_old = catalog.fetch("data", version_id=v1_id)
            assert isinstance(result_old, Path)  # Type narrowing
            old_path = result_old

            # Both should exist with different content
            assert latest_path.read_text() == "new version"
            assert old_path.read_text() == "old version"

            # Should be different cache entries
            assert cache.get("data") is not None  # latest uses dataset name
            # Versioned uses date-based key (filename from path)
            assert (
                cache.get(old_path.name) is not None
            )  # versioned uses date-based filename

    def test_fetch_version_uses_date_based_file_path(self, tmp_path: Path) -> None:
        """Versioned fetches should use date-based file paths (YYYY-MM-DDTHHMMSS.ext)."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )
            version_id = resp["VersionId"]

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch with version_id
            result = catalog.fetch("data", version_id=version_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # File path should be date-based format: YYYY-MM-DDTHHMMSS.ext
            filename = path.name
            assert filename.endswith(".txt")
            # Check format: YYYY-MM-DDTHHMMSS.txt (no colons in time part)
            date_part = filename[:-4]  # Remove .txt extension
            assert (
                len(date_part) == 17
            )  # YYYY-MM-DDTHHMMSS = 17 chars (4+1+2+1+2+1+2+2+2)
            assert date_part[4] == "-"  # Year-month separator
            assert date_part[7] == "-"  # Month-day separator
            assert date_part[10] == "T"  # Date-time separator
            # Time part should have no colons (HHMMSS format)
            assert ":" not in date_part

    def test_fetch_version_date_format_matches_version_timestamp(
        self, tmp_path: Path
    ) -> None:
        """The date in filename should match the version's last_modified timestamp."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )
            version_id = resp["VersionId"]

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            # Get version metadata to check timestamp
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            version_meta = next(v for v in versions if v.version_id == version_id)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch with version_id
            result = catalog.fetch("data", version_id=version_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # Extract date from filename and compare with version timestamp
            filename = path.name
            date_part = filename[:-4]  # Remove .txt extension
            # Parse: YYYY-MM-DDTHHMMSS
            year = int(date_part[0:4])
            month = int(date_part[5:7])
            day = int(date_part[8:10])
            hour = int(date_part[11:13])
            minute = int(date_part[13:15])
            second = int(date_part[15:17])

            from datetime import UTC

            expected_dt = version_meta.last_modified.replace(tzinfo=UTC)
            assert year == expected_dt.year
            assert month == expected_dt.month
            assert day == expected_dt.day
            assert hour == expected_dt.hour
            assert minute == expected_dt.minute
            assert second == expected_dt.second

    def test_fetch_version_preserves_file_extension(self, tmp_path: Path) -> None:
        """The file extension from original source should be preserved."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Test with different extensions
            for ext in [".txt", ".parquet", ".csv", ".json"]:
                key = f"data{ext}"
                resp = client.put_object(
                    Bucket="versioned-bucket", Key=key, Body=b"content"
                )
                version_id = resp["VersionId"]

                cache_dir = tmp_path / "cache"
                storage = S3Storage(client=client)
                cache = FileCache(cache_dir=cache_dir)

                dataset = Dataset(name="data", source=f"s3://versioned-bucket/{key}")
                catalog = Catalog(
                    datasets=[dataset],
                    storage=storage,
                    cache=cache,
                    cache_dir=cache_dir,
                )

                # Fetch with version_id
                result = catalog.fetch("data", version_id=version_id)
                assert isinstance(result, Path)  # Type narrowing
                path = result

                # Extension should be preserved
                assert path.suffix == ext
                assert path.name.endswith(ext)


@pytest.mark.core
@pytest.mark.tra("UseCase.FetchAsOf")
@pytest.mark.tier(1)
class TestFetchAsOf:
    """Tests for fetch() with as_of parameter."""

    @pytest.mark.tier(2)
    def test_fetch_with_as_of_resolves_correct_version(self, tmp_path: Path) -> None:
        """fetch(as_of=datetime) should download version at that time."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload a version
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"version 1"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Get the version timestamp
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            v1_timestamp = versions[0].last_modified

            # Use a time in the future (should get the only version)
            future_time = v1_timestamp + timedelta(days=1)
            result = catalog.fetch("data", as_of=future_time)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            assert path.exists()
            assert path.read_text() == "version 1"

    @pytest.mark.tier(2)
    def test_fetch_with_as_of_uses_version_id_resolution(self, tmp_path: Path) -> None:
        """as_of should resolve to version_id and use _fetch_version."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            as_of = versions[0].last_modified + timedelta(seconds=1)

            # Fetch with as_of
            result = catalog.fetch("data", as_of=as_of)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # Should have cached with date-based key (not {name}@{version_id})
            filename = path.name
            assert filename.endswith(".txt")
            # Should be date-based format: YYYY-MM-DDTHHMMSS.txt
            assert cache.get(filename) is not None

    @pytest.mark.tier(1)
    def test_fetch_as_of_and_version_id_mutually_exclusive(
        self, tmp_path: Path
    ) -> None:
        """fetch() should raise ValueError if both as_of and version_id given."""
        from datetime import datetime

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.txt").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "data.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(ValueError, match="mutually exclusive"):
            catalog.fetch("data", as_of=datetime.now(), version_id="abc123")

    @pytest.mark.tier(1)
    def test_fetch_version_on_glob_raises_error(self, tmp_path: Path) -> None:
        """Versioned fetch on glob dataset should raise clear error."""
        from datetime import datetime

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.txt").write_text("a")
        (storage_dir / "b.txt").write_text("b")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Glob dataset
        dataset = Dataset(name="data", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(ValueError, match="glob"):
            catalog.fetch("data", version_id="abc123")

        with pytest.raises(ValueError, match="glob"):
            catalog.fetch("data", as_of=datetime.now())

    @pytest.mark.tier(2)
    def test_fetch_as_of_raises_version_not_found_if_no_match(
        self, tmp_path: Path
    ) -> None:
        """as_of before any version should raise VersionNotFoundError."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.exceptions import VersionNotFoundError
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"version 1"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Get version timestamp and use a time before it
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            before_all = versions[0].last_modified - timedelta(days=365)

            with pytest.raises(VersionNotFoundError) as exc_info:
                catalog.fetch("data", as_of=before_all)

            assert exc_info.value.name == "data"
            assert exc_info.value.recovery_hint is not None


@pytest.mark.core
@pytest.mark.tra("Domain.ConcurrencyBoundary")
@pytest.mark.tier(1)
class TestConcurrencyBoundary:
    """Tests to verify concurrency boundary compliance in core domain."""

    def test_core_services_no_concurrency_imports(self) -> None:
        """core/services.py should not import ThreadPoolExecutorAdapter or other concurrency primitives."""
        import ast
        from pathlib import Path

        # Read the source file
        core_services_path = (
            Path(__file__).parent.parent.parent
            / "src"
            / "datacachalog"
            / "core"
            / "services.py"
        )
        source_code = core_services_path.read_text()

        # Parse AST to check imports
        tree = ast.parse(source_code, filename=str(core_services_path))

        # Check for forbidden imports
        forbidden_imports = [
            "ThreadPoolExecutorAdapter",
            "ThreadPoolExecutor",
            "ProcessPoolExecutor",
            "threading.Lock",
            "asyncio.Lock",
        ]

        violations: list[str] = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if any(forbidden in alias.name for forbidden in forbidden_imports):
                        violations.append(f"Import: {alias.name}")
            elif isinstance(node, ast.ImportFrom):
                if node.module and any(
                    forbidden in node.module for forbidden in forbidden_imports
                ):
                    violations.append(f"ImportFrom: {node.module}")
                for alias in node.names:
                    if any(forbidden in alias.name for forbidden in forbidden_imports):
                        violations.append(f"ImportFrom: {node.module}.{alias.name}")

        # Also check source code directly for string patterns (AST might miss some)
        if "ThreadPoolExecutorAdapter" in source_code:
            # Check if it's in a comment or string literal vs actual import
            lines = source_code.split("\n")
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                # Skip comments and docstrings, check if it's an import statement
                if (
                    "ThreadPoolExecutorAdapter" in stripped
                    and not stripped.startswith("#")
                    and not stripped.startswith('"""')
                    and "import" in stripped
                ):
                    violations.append(f"Line {i}: {stripped}")

        assert len(violations) == 0, (
            f"Concurrency boundary violations found in core/services.py: {violations}"
        )
