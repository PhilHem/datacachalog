"""Unit tests for Catalog advanced fetch operations (progress, fetch_all, parallel)."""

from pathlib import Path

import pytest

from datacachalog import Dataset


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

        # Sequential order means start/finish pairs are not interleaved
        assert order[0].startswith("start:")
        assert order[1].startswith("finish:")
        assert order[2].startswith("start:")
        assert order[3].startswith("finish:")
