"""Unit tests for Catalog.push() method."""

from pathlib import Path

import pytest

from datacachalog import Dataset
from datacachalog.core.ports import ProgressCallback


@pytest.mark.core
class TestPush:
    """Tests for push() method."""

    def test_push_uploads_to_remote(self, tmp_path: Path) -> None:
        """push() should upload local file to dataset's source location."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup: create directories for "remote" and local
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original content")

        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("updated content")

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
        catalog.push("customers", local_file)

        # Assert: remote file now has updated content
        assert remote_file.read_text() == "updated content"

    def test_push_updates_cache_metadata(self, tmp_path: Path) -> None:
        """push() should update cache with new metadata matching remote."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original")

        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("updated")

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
        catalog.push("customers", local_file)

        # Assert: cache should be fresh (not stale)
        assert catalog.is_stale("customers") is False

    def test_push_allows_fetch_without_redownload(self, tmp_path: Path) -> None:
        """After push(), fetch() should return cache without re-download."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original")

        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("pushed content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Act: push then fetch
        catalog.push("customers", local_file)
        path = catalog.fetch("customers")

        # Assert: fetch returns the pushed content
        assert path.read_text() == "pushed content"

    def test_push_nonexistent_dataset_raises_dataset_not_found(
        self, tmp_path: Path
    ) -> None:
        """push() should raise DatasetNotFoundError for unknown dataset name."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        local_file = tmp_path / "file.csv"
        local_file.write_text("content")

        with pytest.raises(DatasetNotFoundError, match="unknown"):
            catalog.push("unknown", local_file)

    def test_push_nonexistent_file_raises_filenotfounderror(
        self, tmp_path: Path
    ) -> None:
        """push() should raise FileNotFoundError for missing local file."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

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
        )
        catalog = Catalog(
            datasets=[dataset], storage=storage, cache=cache, cache_dir=cache_dir
        )

        missing_file = tmp_path / "does_not_exist.csv"

        with pytest.raises(FileNotFoundError):
            catalog.push("customers", missing_file)

    def test_push_calls_progress_reporter(self, tmp_path: Path) -> None:
        """push() should call progress reporter during upload."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original")

        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        content = "x" * 1000
        local_file.write_text(content)

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(
            name="customers",
            source=str(remote_file),
            cache_path=cache_dir / "customers.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Track progress calls
        started_tasks: list[tuple[str, int]] = []
        finished_tasks: list[str] = []
        progress_calls: list[tuple[int, int]] = []

        class TrackingReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                started_tasks.append((name, total))

                def callback(uploaded: int, total: int) -> None:
                    progress_calls.append((uploaded, total))

                return callback

            def finish_task(self, name: str) -> None:
                finished_tasks.append(name)

        reporter = TrackingReporter()
        catalog.push("customers", local_file, progress=reporter)

        # Assert
        assert ("customers", 1000) in started_tasks
        assert "customers" in finished_tasks
        assert len(progress_calls) > 0
        assert progress_calls[-1][0] == 1000
