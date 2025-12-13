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
        path = catalog.fetch("customers", progress=reporter)

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
        assert result["alpha"].read_text() == "a content"
        assert result["beta"].read_text() == "b content"

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


@pytest.mark.core
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
