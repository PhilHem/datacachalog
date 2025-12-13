"""Unit tests for S3Storage adapter."""

from datetime import datetime
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from datacachalog.core.models import FileMetadata
from datacachalog.core.ports import StoragePort


@pytest.fixture
def s3_client():
    """Create a mocked S3 client with a test bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="test-bucket")
        yield client


@pytest.mark.storage
class TestHead:
    """Tests for head() method."""

    def test_head_returns_file_metadata(self, s3_client) -> None:
        """head() should return FileMetadata with etag, last_modified, and size."""
        from datacachalog.adapters.storage import S3Storage

        # Upload a test object
        s3_client.put_object(
            Bucket="test-bucket", Key="data/test.txt", Body=b"hello world"
        )

        storage = S3Storage(client=s3_client)
        metadata = storage.head("s3://test-bucket/data/test.txt")

        assert isinstance(metadata, FileMetadata)
        assert metadata.etag is not None
        assert metadata.last_modified is not None
        assert isinstance(metadata.last_modified, datetime)
        assert metadata.size == 11  # "hello world" is 11 bytes

    def test_head_etag_from_s3(self, s3_client) -> None:
        """ETag should come directly from S3 response."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="test.txt", Body=b"hello world")

        storage = S3Storage(client=s3_client)
        metadata = storage.head("s3://test-bucket/test.txt")

        # S3 ETags are quoted MD5 hashes for non-multipart uploads
        assert metadata.etag.startswith('"')
        assert metadata.etag.endswith('"')

    def test_head_missing_key_raises(self, s3_client) -> None:
        """head() should raise ClientError for missing keys."""
        from botocore.exceptions import ClientError

        from datacachalog.adapters.storage import S3Storage

        storage = S3Storage(client=s3_client)

        with pytest.raises(ClientError) as exc_info:
            storage.head("s3://test-bucket/nonexistent.txt")

        assert exc_info.value.response["Error"]["Code"] == "404"


@pytest.mark.storage
class TestDownload:
    """Tests for download() method."""

    def test_download_copies_file(self, s3_client, tmp_path: Path) -> None:
        """download() should download file to destination."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="test.txt", Body=b"hello world")
        dest = tmp_path / "downloaded.txt"

        storage = S3Storage(client=s3_client)
        storage.download("s3://test-bucket/test.txt", dest, progress=lambda x, y: None)

        assert dest.exists()
        assert dest.read_text() == "hello world"

    def test_download_reports_progress(self, s3_client, tmp_path: Path) -> None:
        """download() should call progress callback with bytes."""
        from datacachalog.adapters.storage import S3Storage

        content = b"x" * 1000  # 1000 bytes
        s3_client.put_object(Bucket="test-bucket", Key="test.txt", Body=content)
        dest = tmp_path / "downloaded.txt"

        progress_calls: list[tuple[int, int]] = []

        def track_progress(downloaded: int, total: int) -> None:
            progress_calls.append((downloaded, total))

        storage = S3Storage(client=s3_client)
        storage.download("s3://test-bucket/test.txt", dest, progress=track_progress)

        # Should have called progress at least once
        assert len(progress_calls) > 0
        # Final call should show all bytes downloaded
        assert progress_calls[-1][0] == 1000
        assert progress_calls[-1][1] == 1000

    def test_download_missing_key_raises(self, s3_client, tmp_path: Path) -> None:
        """download() should raise ClientError for missing keys."""
        from botocore.exceptions import ClientError

        from datacachalog.adapters.storage import S3Storage

        dest = tmp_path / "downloaded.txt"
        storage = S3Storage(client=s3_client)

        with pytest.raises(ClientError) as exc_info:
            storage.download(
                "s3://test-bucket/nonexistent.txt", dest, progress=lambda x, y: None
            )

        assert exc_info.value.response["Error"]["Code"] == "NoSuchKey"


@pytest.mark.storage
class TestUpload:
    """Tests for upload() method."""

    def test_upload_copies_file(self, s3_client, tmp_path: Path) -> None:
        """upload() should upload local file to S3."""
        from datacachalog.adapters.storage import S3Storage

        local = tmp_path / "local.txt"
        local.write_text("hello world")

        storage = S3Storage(client=s3_client)
        storage.upload(local, "s3://test-bucket/uploaded.txt")

        # Verify file exists in S3
        response = s3_client.get_object(Bucket="test-bucket", Key="uploaded.txt")
        assert response["Body"].read() == b"hello world"

    def test_upload_to_nested_key(self, s3_client, tmp_path: Path) -> None:
        """upload() should handle nested S3 keys."""
        from datacachalog.adapters.storage import S3Storage

        local = tmp_path / "local.txt"
        local.write_text("hello world")

        storage = S3Storage(client=s3_client)
        storage.upload(local, "s3://test-bucket/path/to/uploaded.txt")

        # Verify file exists in S3
        response = s3_client.get_object(
            Bucket="test-bucket", Key="path/to/uploaded.txt"
        )
        assert response["Body"].read() == b"hello world"


@pytest.mark.storage
class TestProtocolConformance:
    """Tests for StoragePort protocol conformance."""

    def test_satisfies_storage_port(self, s3_client) -> None:
        """S3Storage should satisfy StoragePort protocol."""
        from datacachalog.adapters.storage import S3Storage

        storage = S3Storage(client=s3_client)
        assert isinstance(storage, StoragePort)


@pytest.mark.storage
class TestUriParsing:
    """Tests for S3 URI parsing."""

    def test_parses_simple_uri(self, s3_client) -> None:
        """Should parse s3://bucket/key correctly."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="file.txt", Body=b"test")

        storage = S3Storage(client=s3_client)
        metadata = storage.head("s3://test-bucket/file.txt")

        assert metadata.size == 4

    def test_parses_nested_key(self, s3_client) -> None:
        """Should parse s3://bucket/path/to/file correctly."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="path/to/file.txt", Body=b"test")

        storage = S3Storage(client=s3_client)
        metadata = storage.head("s3://test-bucket/path/to/file.txt")

        assert metadata.size == 4
