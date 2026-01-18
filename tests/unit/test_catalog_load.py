"""Unit tests for Catalog.load() method.

Test Isolation Strategy:
- Each test uses pytest's tmp_path fixture for unique directories
- FileCache instances are created fresh per test with isolated cache_dir
- No shared state between tests
"""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datacachalog import Dataset
from datacachalog.adapters.cache import FileCache
from datacachalog.adapters.storage import FilesystemStorage
from datacachalog.core.exceptions import ReaderNotConfiguredError
from datacachalog.core.services import Catalog


@pytest.fixture
def storage() -> FilesystemStorage:
    """Stateless filesystem storage adapter."""
    return FilesystemStorage()


@pytest.fixture
def cache(tmp_path: Path) -> FileCache:
    """Isolated file cache using tmp_path for test isolation."""
    cache_dir = tmp_path / "cache"
    return FileCache(cache_dir=cache_dir)


@pytest.mark.core
@pytest.mark.tra("UseCase.Load")
@pytest.mark.tier(1)
class TestLoad:
    """Tests for load() method."""

    def test_load_single_file(
        self, tmp_path: Path, storage: FilesystemStorage, cache: FileCache
    ) -> None:
        """load() should fetch dataset and call reader.read() with the path."""
        # Setup: create a "remote" file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        # Create a simple reader that tracks calls
        class TrackingReader:
            def __init__(self) -> None:
                self.read_calls: list[Path] = []

            def read(self, path: Path) -> str:
                self.read_calls.append(path)
                return path.read_text()

        reader = TrackingReader()
        cache_dir = tmp_path / "cache"

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, reader=reader
        )

        result = catalog.load("customers")

        # Verify reader was called with the cached path
        assert len(reader.read_calls) == 1
        assert reader.read_calls[0].exists()
        # Verify result is what reader returned
        assert result == "id,name\n1,Alice\n"

    def test_load_raises_without_reader(
        self, tmp_path: Path, storage: FilesystemStorage, cache: FileCache
    ) -> None:
        """load() should raise ReaderNotConfiguredError when no reader configured."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        # Catalog without reader
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        with pytest.raises(ReaderNotConfiguredError, match="customers"):
            catalog.load("customers")

    def test_load_passes_fetch_params(
        self, tmp_path: Path, storage: FilesystemStorage, cache: FileCache
    ) -> None:
        """load() should pass version_id, as_of, progress through to fetch()."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"

        class SimpleReader:
            def read(self, path: Path) -> str:
                return path.read_text()

        reader = SimpleReader()

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, reader=reader
        )

        # Create a mock progress reporter
        progress = MagicMock()
        progress.start_task.return_value = lambda x, y: None
        progress.finish_task.return_value = None

        # Call load with progress - verify no exception and result returned
        result = catalog.load("customers", progress=progress)
        assert result == "id,name\n1,Alice\n"

        # Verify progress reporter was used (start_task called)
        progress.start_task.assert_called()

    def test_load_dry_run_returns_path(
        self, tmp_path: Path, storage: FilesystemStorage, cache: FileCache
    ) -> None:
        """load() with dry_run=True should return Path without calling reader."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("id,name\n1,Alice\n")

        cache_dir = tmp_path / "cache"

        class TrackingReader:
            def __init__(self) -> None:
                self.read_calls: list[Path] = []

            def read(self, path: Path) -> str:
                self.read_calls.append(path)
                return path.read_text()

        reader = TrackingReader()

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, reader=reader
        )

        result = catalog.load("customers", dry_run=True)

        # Verify dry_run returns Path, not reader result
        assert isinstance(result, Path)
        # Verify reader was NOT called
        assert len(reader.read_calls) == 0

    def test_load_glob_returns_list(self, tmp_path: Path) -> None:
        """load() should call reader.read() for each file in glob pattern."""
        # Setup: create multiple files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "2024-01.csv").write_text("jan")
        (storage_dir / "2024-02.csv").write_text("feb")
        (storage_dir / "2024-03.csv").write_text("mar")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        class TrackingReader:
            def __init__(self) -> None:
                self.read_calls: list[Path] = []

            def read(self, path: Path) -> str:
                self.read_calls.append(path)
                return path.read_text()

        reader = TrackingReader()

        # Dataset with glob pattern
        dataset = Dataset(
            name="monthly",
            source=str(storage_dir / "*.csv"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
            reader=reader,
        )

        result = catalog.load("monthly")

        # Verify result is a list of loaded values
        assert isinstance(result, list)
        assert len(result) == 3
        assert set(result) == {"jan", "feb", "mar"}
        # Verify reader was called for each file
        assert len(reader.read_calls) == 3

    def test_load_glob_maintains_order(self, tmp_path: Path) -> None:
        """load() should maintain the order of files from fetch()."""
        # Setup: create files with known order (alphabetical by default)
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.csv").write_text("alpha")
        (storage_dir / "b.csv").write_text("beta")
        (storage_dir / "c.csv").write_text("gamma")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        class OrderedReader:
            def __init__(self) -> None:
                self.order: list[str] = []

            def read(self, path: Path) -> str:
                content = path.read_text()
                self.order.append(content)
                return content

        reader = OrderedReader()

        dataset = Dataset(
            name="files",
            source=str(storage_dir / "*.csv"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
            reader=reader,
        )

        # Fetch first to get the order
        fetch_result = catalog.fetch("files")
        assert isinstance(fetch_result, list)
        fetch_order = [p.read_text() for p in fetch_result]

        # Clear reader for load test
        reader.order.clear()

        # Load should maintain the same order
        result = catalog.load("files")
        assert isinstance(result, list)

        # Verify the order matches fetch order
        assert result == fetch_order
        assert reader.order == fetch_order
