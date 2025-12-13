"""Unit tests for FilesystemStorage adapter."""

from datetime import datetime
from pathlib import Path

import pytest

from datacachalog.core.models import FileMetadata
from datacachalog.core.ports import StoragePort


@pytest.mark.storage
class TestHead:
    """Tests for head() method."""

    def test_head_returns_file_metadata(self, tmp_path: Path) -> None:
        """head() should return FileMetadata with etag and last_modified."""
        from datacachalog.adapters.storage import FilesystemStorage

        # Create a test file
        source = tmp_path / "test.txt"
        source.write_text("hello world")

        storage = FilesystemStorage()
        metadata = storage.head(str(source))

        assert isinstance(metadata, FileMetadata)
        assert metadata.etag is not None
        assert metadata.last_modified is not None
        assert isinstance(metadata.last_modified, datetime)
        assert metadata.size == 11  # "hello world" is 11 bytes

    def test_head_etag_is_md5_hash(self, tmp_path: Path) -> None:
        """ETag should be MD5 hash of file contents, quoted like S3."""
        import hashlib

        from datacachalog.adapters.storage import FilesystemStorage

        source = tmp_path / "test.txt"
        content = b"hello world"
        source.write_bytes(content)

        expected_md5 = hashlib.md5(content).hexdigest()

        storage = FilesystemStorage()
        metadata = storage.head(str(source))

        # S3 ETags are quoted
        assert metadata.etag == f'"{expected_md5}"'

    def test_head_file_not_found_raises_storage_not_found(self, tmp_path: Path) -> None:
        """head() should raise StorageNotFoundError for missing files."""
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import StorageNotFoundError

        storage = FilesystemStorage()

        with pytest.raises(StorageNotFoundError) as exc_info:
            storage.head(str(tmp_path / "nonexistent.txt"))

        assert "nonexistent.txt" in exc_info.value.source
        assert exc_info.value.recovery_hint is not None


@pytest.mark.storage
class TestDownload:
    """Tests for download() method."""

    def test_download_copies_file(self, tmp_path: Path) -> None:
        """download() should copy file to destination."""
        from datacachalog.adapters.storage import FilesystemStorage

        source = tmp_path / "source.txt"
        dest = tmp_path / "dest.txt"
        source.write_text("hello world")

        storage = FilesystemStorage()
        storage.download(str(source), dest, progress=lambda x, y: None)

        assert dest.exists()
        assert dest.read_text() == "hello world"

    def test_download_reports_progress(self, tmp_path: Path) -> None:
        """download() should call progress callback with bytes."""
        from datacachalog.adapters.storage import FilesystemStorage

        source = tmp_path / "source.txt"
        dest = tmp_path / "dest.txt"
        content = "x" * 1000  # 1000 bytes
        source.write_text(content)

        progress_calls: list[tuple[int, int]] = []

        def track_progress(downloaded: int, total: int) -> None:
            progress_calls.append((downloaded, total))

        storage = FilesystemStorage()
        storage.download(str(source), dest, progress=track_progress)

        # Should have called progress at least once
        assert len(progress_calls) > 0
        # Final call should show all bytes downloaded
        assert progress_calls[-1][0] == 1000
        assert progress_calls[-1][1] == 1000

    def test_download_source_not_found_raises_storage_not_found(
        self, tmp_path: Path
    ) -> None:
        """download() should raise StorageNotFoundError for missing source."""
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import StorageNotFoundError

        dest = tmp_path / "dest.txt"

        storage = FilesystemStorage()

        with pytest.raises(StorageNotFoundError) as exc_info:
            storage.download(
                str(tmp_path / "nonexistent.txt"), dest, progress=lambda x, y: None
            )

        assert "nonexistent.txt" in exc_info.value.source


@pytest.mark.storage
class TestUpload:
    """Tests for upload() method."""

    def test_upload_copies_file(self, tmp_path: Path) -> None:
        """upload() should copy local file to destination."""
        from datacachalog.adapters.storage import FilesystemStorage

        local = tmp_path / "local.txt"
        dest = tmp_path / "remote.txt"
        local.write_text("hello world")

        storage = FilesystemStorage()
        storage.upload(local, str(dest))

        assert dest.exists()
        assert dest.read_text() == "hello world"

    def test_upload_creates_parent_directories(self, tmp_path: Path) -> None:
        """upload() should create parent directories if needed."""
        from datacachalog.adapters.storage import FilesystemStorage

        local = tmp_path / "local.txt"
        dest = tmp_path / "nested" / "path" / "remote.txt"
        local.write_text("hello world")

        storage = FilesystemStorage()
        storage.upload(local, str(dest))

        assert dest.exists()
        assert dest.read_text() == "hello world"

    def test_upload_reports_progress(self, tmp_path: Path) -> None:
        """upload() should call progress callback with bytes."""
        from datacachalog.adapters.storage import FilesystemStorage

        local = tmp_path / "local.txt"
        dest = tmp_path / "remote.txt"
        content = "x" * 1000
        local.write_text(content)

        progress_calls: list[tuple[int, int]] = []

        def track_progress(uploaded: int, total: int) -> None:
            progress_calls.append((uploaded, total))

        storage = FilesystemStorage()
        storage.upload(local, str(dest), progress=track_progress)

        assert len(progress_calls) > 0
        assert progress_calls[-1][0] == 1000
        assert progress_calls[-1][1] == 1000


@pytest.mark.storage
class TestProtocolConformance:
    """Tests for StoragePort protocol conformance."""

    def test_satisfies_storage_port(self) -> None:
        """FilesystemStorage should satisfy StoragePort protocol."""
        from datacachalog.adapters.storage import FilesystemStorage

        storage = FilesystemStorage()
        assert isinstance(storage, StoragePort)
