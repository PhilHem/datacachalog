"""Tests for RouterStorage composite adapter."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from datacachalog.core.models import FileMetadata


if TYPE_CHECKING:
    from pathlib import Path


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestParseUriScheme:
    """Tests for URI scheme extraction."""

    def test_extracts_s3_scheme(self) -> None:
        """Should extract 's3' from s3://bucket/key."""
        from datacachalog.adapters.storage.router import parse_uri_scheme

        assert parse_uri_scheme("s3://bucket/path/file.csv") == "s3"

    def test_extracts_file_scheme(self) -> None:
        """Should extract 'file' from file:///path."""
        from datacachalog.adapters.storage.router import parse_uri_scheme

        assert parse_uri_scheme("file:///local/path/file.csv") == "file"

    def test_returns_none_for_absolute_path(self) -> None:
        """Should return None for absolute paths without scheme."""
        from datacachalog.adapters.storage.router import parse_uri_scheme

        assert parse_uri_scheme("/local/path/file.csv") is None

    def test_returns_none_for_relative_path(self) -> None:
        """Should return None for relative paths."""
        from datacachalog.adapters.storage.router import parse_uri_scheme

        assert parse_uri_scheme("relative/path/file.csv") is None

    def test_returns_none_for_windows_path(self) -> None:
        """Should return None for Windows paths (not confuse C: with scheme)."""
        from datacachalog.adapters.storage.router import parse_uri_scheme

        assert parse_uri_scheme("C:/Users/data/file.csv") is None
        assert parse_uri_scheme("D:\\Documents\\file.csv") is None

    def test_normalizes_scheme_to_lowercase(self) -> None:
        """Should normalize scheme to lowercase."""
        from datacachalog.adapters.storage.router import parse_uri_scheme

        assert parse_uri_scheme("S3://bucket/key") == "s3"
        assert parse_uri_scheme("FILE:///path") == "file"


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestRouterStorageHead:
    """Tests for RouterStorage.head() routing."""

    def test_routes_s3_uri_to_s3_storage(self) -> None:
        """head() should delegate s3:// URIs to S3Storage."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_s3 = Mock()
        mock_s3.head.return_value = FileMetadata(
            etag='"abc"', last_modified=datetime.now(UTC), size=100
        )

        router = RouterStorage(backends={"s3": mock_s3})
        result = router.head("s3://bucket/key.csv")

        mock_s3.head.assert_called_once_with("s3://bucket/key.csv")
        assert result.etag == '"abc"'

    def test_routes_local_path_to_default_storage(self) -> None:
        """head() should delegate local paths to default (None) backend."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_fs = Mock()
        mock_fs.head.return_value = FileMetadata(
            etag='"xyz"', last_modified=datetime.now(UTC), size=200
        )

        router = RouterStorage(backends={None: mock_fs})
        result = router.head("/local/path/file.csv")

        mock_fs.head.assert_called_once_with("/local/path/file.csv")
        assert result.etag == '"xyz"'

    def test_raises_for_unknown_scheme(self) -> None:
        """head() should raise ValueError for unregistered schemes."""
        from datacachalog.adapters.storage.router import RouterStorage

        router = RouterStorage(backends={})

        with pytest.raises(
            ValueError, match="No storage backend registered for scheme"
        ):
            router.head("gcs://bucket/file.csv")

    def test_raises_for_local_without_default(self) -> None:
        """head() should raise ValueError for local path without default backend."""
        from datacachalog.adapters.storage.router import RouterStorage

        router = RouterStorage(backends={"s3": Mock()})

        with pytest.raises(ValueError, match="No storage backend registered"):
            router.head("/local/path/file.csv")


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestRouterStorageDownload:
    """Tests for RouterStorage.download() routing."""

    def test_routes_download_to_correct_backend(self, tmp_path: Path) -> None:
        """download() should delegate to correct backend based on scheme."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_s3 = Mock()
        router = RouterStorage(backends={"s3": mock_s3})

        dest = tmp_path / "file.csv"

        def progress(current: int, total: int) -> None:
            pass

        router.download("s3://bucket/key.csv", dest, progress)

        mock_s3.download.assert_called_once_with("s3://bucket/key.csv", dest, progress)


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestRouterStorageUpload:
    """Tests for RouterStorage.upload() routing."""

    def test_routes_upload_to_correct_backend(self, tmp_path: Path) -> None:
        """upload() should delegate to correct backend based on dest scheme."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_s3 = Mock()
        router = RouterStorage(backends={"s3": mock_s3})

        local = tmp_path / "local.csv"
        local.write_text("data")

        router.upload(local, "s3://bucket/key.csv")

        mock_s3.upload.assert_called_once_with(local, "s3://bucket/key.csv", None)


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestRouterStorageProtocol:
    """Tests for RouterStorage protocol conformance."""

    def test_satisfies_storage_port(self) -> None:
        """RouterStorage should satisfy StoragePort protocol."""
        from datacachalog.adapters.storage.router import RouterStorage
        from datacachalog.core.ports import StoragePort

        router = RouterStorage(backends={})
        assert isinstance(router, StoragePort)


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestCreateRouter:
    """Tests for create_router() factory function."""

    def test_creates_router_with_default_backends(self) -> None:
        """create_router() should create RouterStorage with S3 and filesystem."""
        from datacachalog.adapters.storage import FilesystemStorage, S3Storage
        from datacachalog.adapters.storage.router import create_router

        router = create_router()

        assert "s3" in router._backends
        assert None in router._backends
        assert isinstance(router._backends["s3"], S3Storage)
        assert isinstance(router._backends[None], FilesystemStorage)

    def test_accepts_custom_s3_client(self) -> None:
        """create_router() should accept custom boto3 client for S3."""
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.adapters.storage.router import create_router

        mock_client = Mock()
        router = create_router(s3_client=mock_client)

        s3_backend = router._backends["s3"]
        assert isinstance(s3_backend, S3Storage)  # Type narrowing
        assert s3_backend._client is mock_client


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestFileUriSupport:
    """Tests for file:// URI scheme support."""

    def test_routes_file_uri_to_filesystem_storage(self) -> None:
        """file:// URIs should route to FilesystemStorage with path stripped."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_fs = Mock()
        mock_fs.head.return_value = FileMetadata(
            etag='"abc"', last_modified=datetime.now(UTC), size=100
        )

        router = RouterStorage(backends={"file": mock_fs})
        router.head("file:///local/path/file.csv")

        # Should strip file:// prefix before delegating
        mock_fs.head.assert_called_once_with("/local/path/file.csv")

    def test_create_router_includes_file_scheme(self) -> None:
        """create_router() should register 'file' scheme to FilesystemStorage."""
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.adapters.storage.router import create_router

        router = create_router()

        assert "file" in router._backends
        assert isinstance(router._backends["file"], FilesystemStorage)


@pytest.mark.storage
@pytest.mark.tra("Adapter.RouterStorage")
@pytest.mark.tier(1)
class TestRouterStorageList:
    """Tests for RouterStorage.list() routing."""

    def test_routes_list_to_s3_backend(self) -> None:
        """list() should delegate s3:// URIs to S3Storage."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_s3 = Mock()
        mock_s3.list.return_value = [
            "s3://bucket/data/a.parquet",
            "s3://bucket/data/b.parquet",
        ]

        router = RouterStorage(backends={"s3": mock_s3})
        result = router.list("s3://bucket/data/", pattern="*.parquet")

        mock_s3.list.assert_called_once_with("s3://bucket/data/", "*.parquet")
        assert len(result) == 2

    def test_routes_list_to_filesystem_backend(self) -> None:
        """list() should delegate local paths to filesystem backend."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_fs = Mock()
        mock_fs.list.return_value = ["/data/a.txt", "/data/b.txt"]

        router = RouterStorage(backends={None: mock_fs})
        result = router.list("/data/")

        mock_fs.list.assert_called_once_with("/data/", None)
        assert len(result) == 2

    def test_strips_file_scheme_for_list(self) -> None:
        """list() should strip file:// prefix before delegating."""
        from datacachalog.adapters.storage.router import RouterStorage

        mock_fs = Mock()
        mock_fs.list.return_value = ["/local/data/a.txt"]

        router = RouterStorage(backends={"file": mock_fs})
        router.list("file:///local/data/", pattern="*.txt")

        mock_fs.list.assert_called_once_with("/local/data/", "*.txt")
