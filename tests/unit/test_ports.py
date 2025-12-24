"""Unit tests for port interfaces."""

import pytest


@pytest.mark.core
@pytest.mark.tra("Port.ProgressCallback")
@pytest.mark.tier(0)
def test_progress_callback_is_callable():
    """ProgressCallback should be a callable type alias."""
    from datacachalog.core.ports import ProgressCallback

    # ProgressCallback should be importable as a type
    assert ProgressCallback is not None


@pytest.mark.core
@pytest.mark.tra("Port.StoragePort")
@pytest.mark.tier(0)
def test_storage_port_has_download_method():
    """StoragePort should have a download method."""
    from datacachalog.core.ports import StoragePort

    assert hasattr(StoragePort, "download")


@pytest.mark.core
@pytest.mark.tra("Port.StoragePort")
@pytest.mark.tier(0)
def test_storage_port_has_upload_method():
    """StoragePort should have an upload method."""
    from datacachalog.core.ports import StoragePort

    assert hasattr(StoragePort, "upload")


@pytest.mark.core
@pytest.mark.tra("Port.StoragePort")
@pytest.mark.tier(0)
def test_storage_port_has_head_method():
    """StoragePort should have a head method."""
    from datacachalog.core.ports import StoragePort

    assert hasattr(StoragePort, "head")


@pytest.mark.core
@pytest.mark.tra("Port.CachePort")
@pytest.mark.tier(0)
def test_cache_port_has_get_method():
    """CachePort should have a get method."""
    from datacachalog.core.ports import CachePort

    assert hasattr(CachePort, "get")


@pytest.mark.core
@pytest.mark.tra("Port.CachePort")
@pytest.mark.tier(0)
def test_cache_port_has_put_method():
    """CachePort should have a put method."""
    from datacachalog.core.ports import CachePort

    assert hasattr(CachePort, "put")


@pytest.mark.core
@pytest.mark.tra("Port.CachePort")
@pytest.mark.tier(0)
def test_cache_port_has_invalidate_method():
    """CachePort should have an invalidate method."""
    from datacachalog.core.ports import CachePort

    assert hasattr(CachePort, "invalidate")


@pytest.mark.core
@pytest.mark.tra("Port.StoragePort")
@pytest.mark.tier(0)
def test_class_satisfies_storage_port(fake_storage):
    """A class with matching methods should satisfy StoragePort via isinstance."""
    from datacachalog.core.ports import StoragePort

    assert isinstance(fake_storage, StoragePort)


@pytest.mark.core
@pytest.mark.tra("Port.CachePort")
@pytest.mark.tier(0)
def test_class_satisfies_cache_port():
    """A class with matching methods should satisfy CachePort via isinstance."""
    import builtins
    from pathlib import Path

    from datacachalog.core.models import CacheMetadata
    from datacachalog.core.ports import CachePort

    class FakeCache:
        def get(self, key: str) -> tuple[Path, CacheMetadata] | None:
            return None

        def put(self, key: str, path: Path, metadata: CacheMetadata) -> None:
            pass

        def invalidate(self, key: str) -> None:
            pass

        def invalidate_prefix(self, prefix: str) -> int:
            return 0

        def list_all_keys(self) -> builtins.list[str]:
            return []

    cache: CachePort = FakeCache()
    assert isinstance(cache, CachePort)


@pytest.mark.core
@pytest.mark.tra("Port.CachePort")
@pytest.mark.tier(0)
class TestListAllKeys:
    """Tests for list_all_keys() method in CachePort protocol."""

    def test_cache_port_has_list_all_keys_method(self) -> None:
        """CachePort should have a list_all_keys method."""
        from datacachalog.core.ports import CachePort

        assert hasattr(CachePort, "list_all_keys")

    def test_fake_cache_satisfies_cache_port_with_list_all_keys(self) -> None:
        """A fake cache with list_all_keys() should satisfy CachePort protocol."""
        import builtins
        from pathlib import Path

        from datacachalog.core.models import CacheMetadata
        from datacachalog.core.ports import CachePort

        class FakeCache:
            def get(self, key: str) -> tuple[Path, CacheMetadata] | None:
                return None

            def put(self, key: str, path: Path, metadata: CacheMetadata) -> None:
                pass

            def invalidate(self, key: str) -> None:
                pass

            def invalidate_prefix(self, prefix: str) -> int:
                return 0

            def list_all_keys(self) -> builtins.list[str]:
                return []

        cache: CachePort = FakeCache()
        assert isinstance(cache, CachePort)


@pytest.mark.core
@pytest.mark.tra("Port.StoragePort")
@pytest.mark.tier(0)
def test_ports_exported_from_package():
    """Ports should be importable from the main package."""
    from datacachalog import CachePort, ProgressCallback, StoragePort

    assert StoragePort is not None
    assert CachePort is not None
    assert ProgressCallback is not None


@pytest.mark.core
@pytest.mark.tra("Port.ProgressReporter")
@pytest.mark.tier(0)
class TestProgressReporterProtocol:
    """Tests for ProgressReporter protocol definition."""

    def test_progress_reporter_protocol_exists(self) -> None:
        """ProgressReporter should be importable from core.ports."""
        from datacachalog.core.ports import ProgressReporter

        assert ProgressReporter is not None

    def test_progress_reporter_is_runtime_checkable(self) -> None:
        """ProgressReporter should support isinstance checks."""
        from datacachalog.core.ports import ProgressCallback, ProgressReporter

        class DummyReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                return lambda downloaded, total: None

            def finish_task(self, name: str) -> None:
                pass

        assert isinstance(DummyReporter(), ProgressReporter)

    def test_start_task_returns_progress_callback(self) -> None:
        """start_task() should return a ProgressCallback."""
        from datacachalog.core.ports import ProgressCallback, ProgressReporter

        class DummyReporter:
            def start_task(self, name: str, total: int) -> ProgressCallback:
                return lambda downloaded, total: None

            def finish_task(self, name: str) -> None:
                pass

        reporter: ProgressReporter = DummyReporter()
        callback = reporter.start_task("test", 1000)
        # Verify it's callable with (int, int)
        callback(500, 1000)  # Should not raise


@pytest.mark.core
@pytest.mark.tra("Port.ProgressReporter")
@pytest.mark.tier(0)
class TestNullProgressReporter:
    """Tests for NullProgressReporter."""

    def test_null_reporter_exists(self) -> None:
        """NullProgressReporter should be importable from core.ports."""
        from datacachalog.core.ports import NullProgressReporter

        reporter = NullProgressReporter()
        assert reporter is not None

    def test_null_reporter_satisfies_protocol(self) -> None:
        """NullProgressReporter should implement ProgressReporter."""
        from datacachalog.core.ports import NullProgressReporter, ProgressReporter

        reporter = NullProgressReporter()
        assert isinstance(reporter, ProgressReporter)

    def test_start_task_returns_noop_callback(self) -> None:
        """start_task() should return a callable that does nothing."""
        from datacachalog.core.ports import NullProgressReporter

        reporter = NullProgressReporter()
        callback = reporter.start_task("test", 1000)

        # Should not raise
        callback(100, 1000)
        callback(500, 1000)
        callback(1000, 1000)

    def test_finish_task_is_noop(self) -> None:
        """finish_task() should do nothing without error."""
        from datacachalog.core.ports import NullProgressReporter

        reporter = NullProgressReporter()
        reporter.finish_task("test")  # Should not raise
