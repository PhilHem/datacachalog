"""Unit tests for RichProgressReporter adapter."""

from pathlib import Path

import pytest


@pytest.mark.progress
class TestRichProgressReporter:
    """Tests for RichProgressReporter."""

    def test_rich_reporter_exists(self) -> None:
        """RichProgressReporter should be importable from progress module."""
        from datacachalog.progress import RichProgressReporter

        reporter = RichProgressReporter()
        assert reporter is not None

    def test_rich_reporter_satisfies_protocol(self) -> None:
        """RichProgressReporter should implement ProgressReporter."""
        from datacachalog.core.ports import ProgressReporter
        from datacachalog.progress import RichProgressReporter

        reporter = RichProgressReporter()
        assert isinstance(reporter, ProgressReporter)

    def test_start_task_returns_callable(self) -> None:
        """start_task() should return a callable progress callback."""
        from datacachalog.progress import RichProgressReporter

        reporter = RichProgressReporter()
        callback = reporter.start_task("test", 1000)

        # Should be callable
        assert callable(callback)

        # Should accept (int, int) without error
        callback(100, 1000)

    def test_finish_task_completes_without_error(self) -> None:
        """finish_task() should complete without error."""
        from datacachalog.progress import RichProgressReporter

        reporter = RichProgressReporter()
        reporter.start_task("test", 1000)
        reporter.finish_task("test")  # Should not raise

    def test_multiple_concurrent_tasks(self) -> None:
        """Reporter should handle multiple concurrent tasks."""
        from datacachalog.progress import RichProgressReporter

        reporter = RichProgressReporter()

        cb1 = reporter.start_task("file1", 1000)
        cb2 = reporter.start_task("file2", 2000)

        # Update both
        cb1(500, 1000)
        cb2(1000, 2000)

        # Finish both
        reporter.finish_task("file1")
        reporter.finish_task("file2")

    def test_context_manager_usage(self) -> None:
        """Reporter should support context manager for clean shutdown."""
        from datacachalog.progress import RichProgressReporter

        with RichProgressReporter() as reporter:
            callback = reporter.start_task("test", 100)
            callback(50, 100)
            reporter.finish_task("test")

        # Should exit cleanly


@pytest.mark.progress
class TestRichProgressReporterIntegration:
    """Integration tests for RichProgressReporter with Catalog."""

    def test_catalog_fetch_with_rich_progress(self, tmp_path: Path) -> None:
        """Catalog.fetch() should work with RichProgressReporter."""
        from datacachalog import Dataset
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog
        from datacachalog.progress import RichProgressReporter

        # Setup
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("x" * 500)

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(
            name="test",
            source=str(remote_file),
            cache_path=cache_dir / "test.csv",
        )
        catalog = Catalog(datasets=[dataset], storage=storage, cache=cache)

        # Act
        with RichProgressReporter() as reporter:
            result = catalog.fetch("test", progress=reporter)
            assert isinstance(result, Path)  # Type narrowing
            path = result

        # Assert
        assert path.exists()
        assert path.read_text() == "x" * 500
