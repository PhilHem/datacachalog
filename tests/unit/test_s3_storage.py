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


@pytest.fixture
def versioned_s3_client():
    """Create a mocked S3 client with a versioned bucket."""
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="versioned-bucket")
        # Enable versioning on the bucket
        client.put_bucket_versioning(
            Bucket="versioned-bucket",
            VersioningConfiguration={"Status": "Enabled"},
        )
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

    def test_head_missing_key_raises_storage_not_found(self, s3_client) -> None:
        """head() should raise StorageNotFoundError for missing keys."""
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.exceptions import StorageNotFoundError

        storage = S3Storage(client=s3_client)

        with pytest.raises(StorageNotFoundError) as exc_info:
            storage.head("s3://test-bucket/nonexistent.txt")

        assert "nonexistent.txt" in exc_info.value.source
        assert exc_info.value.recovery_hint is not None


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

    def test_download_missing_key_raises_storage_not_found(
        self, s3_client, tmp_path: Path
    ) -> None:
        """download() should raise StorageNotFoundError for missing keys."""
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.exceptions import StorageNotFoundError

        dest = tmp_path / "downloaded.txt"
        storage = S3Storage(client=s3_client)

        with pytest.raises(StorageNotFoundError) as exc_info:
            storage.download(
                "s3://test-bucket/nonexistent.txt", dest, progress=lambda x, y: None
            )

        assert "nonexistent.txt" in exc_info.value.source


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

    def test_upload_reports_progress(self, s3_client, tmp_path: Path) -> None:
        """upload() should call progress callback with bytes."""
        from datacachalog.adapters.storage import S3Storage

        local = tmp_path / "local.txt"
        content = "x" * 1000
        local.write_text(content)

        progress_calls: list[tuple[int, int]] = []

        def track_progress(uploaded: int, total: int) -> None:
            progress_calls.append((uploaded, total))

        storage = S3Storage(client=s3_client)
        storage.upload(local, "s3://test-bucket/uploaded.txt", progress=track_progress)

        assert len(progress_calls) > 0
        assert progress_calls[-1][0] == 1000
        assert progress_calls[-1][1] == 1000


@pytest.mark.storage
class TestList:
    """Tests for list() method."""

    def test_list_returns_objects_with_prefix(self, s3_client) -> None:
        """list() should return all objects matching prefix."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="data/a.parquet", Body=b"a")
        s3_client.put_object(Bucket="test-bucket", Key="data/b.parquet", Body=b"b")
        s3_client.put_object(Bucket="test-bucket", Key="other/c.parquet", Body=b"c")

        storage = S3Storage(client=s3_client)
        result = storage.list("s3://test-bucket/data/")

        assert len(result) == 2
        assert "s3://test-bucket/data/a.parquet" in result
        assert "s3://test-bucket/data/b.parquet" in result
        assert "s3://test-bucket/other/c.parquet" not in result

    def test_list_with_pattern_filters_by_glob(self, s3_client) -> None:
        """list() with pattern should filter by glob."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="data/a.parquet", Body=b"a")
        s3_client.put_object(Bucket="test-bucket", Key="data/b.parquet", Body=b"b")
        s3_client.put_object(Bucket="test-bucket", Key="data/c.csv", Body=b"c")

        storage = S3Storage(client=s3_client)
        result = storage.list("s3://test-bucket/data/", pattern="*.parquet")

        assert len(result) == 2
        assert "s3://test-bucket/data/a.parquet" in result
        assert "s3://test-bucket/data/b.parquet" in result
        assert "s3://test-bucket/data/c.csv" not in result

    def test_list_returns_sorted_alphabetically(self, s3_client) -> None:
        """list() should return results sorted alphabetically."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="data/z.txt", Body=b"z")
        s3_client.put_object(Bucket="test-bucket", Key="data/a.txt", Body=b"a")
        s3_client.put_object(Bucket="test-bucket", Key="data/m.txt", Body=b"m")

        storage = S3Storage(client=s3_client)
        result = storage.list("s3://test-bucket/data/")

        assert result == [
            "s3://test-bucket/data/a.txt",
            "s3://test-bucket/data/m.txt",
            "s3://test-bucket/data/z.txt",
        ]

    def test_list_empty_prefix_returns_empty_list(self, s3_client) -> None:
        """list() with no matching objects should return empty list."""
        from datacachalog.adapters.storage import S3Storage

        storage = S3Storage(client=s3_client)
        result = storage.list("s3://test-bucket/nonexistent/")

        assert result == []

    def test_list_handles_pagination(self, s3_client) -> None:
        """list() should handle paginated results."""
        from datacachalog.adapters.storage import S3Storage

        # Create more objects than default page size
        for i in range(25):
            s3_client.put_object(
                Bucket="test-bucket", Key=f"data/file{i:02d}.txt", Body=b"x"
            )

        storage = S3Storage(client=s3_client)
        result = storage.list("s3://test-bucket/data/")

        assert len(result) == 25

    def test_list_recursive_pattern(self, s3_client) -> None:
        """list() with ** pattern should match nested keys."""
        from datacachalog.adapters.storage import S3Storage

        s3_client.put_object(Bucket="test-bucket", Key="data/a.parquet", Body=b"a")
        s3_client.put_object(Bucket="test-bucket", Key="data/sub/b.parquet", Body=b"b")
        s3_client.put_object(
            Bucket="test-bucket", Key="data/sub/deep/c.parquet", Body=b"c"
        )
        s3_client.put_object(Bucket="test-bucket", Key="data/other.csv", Body=b"x")

        storage = S3Storage(client=s3_client)
        result = storage.list("s3://test-bucket/data/", pattern="**/*.parquet")

        # S3 is flat, so ** should match all .parquet at any depth
        assert len(result) == 3
        assert "s3://test-bucket/data/a.parquet" in result
        assert "s3://test-bucket/data/sub/b.parquet" in result
        assert "s3://test-bucket/data/sub/deep/c.parquet" in result


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


@pytest.mark.storage
class TestPackageExport:
    """Tests for S3Storage export from package root."""

    def test_s3storage_exported_from_package_root(self) -> None:
        """S3Storage should be importable from datacachalog."""
        from datacachalog import S3Storage

        assert S3Storage is not None


@pytest.mark.storage
class TestListVersions:
    """Tests for list_versions() method."""

    def test_list_versions_returns_object_versions(self, versioned_s3_client) -> None:
        """list_versions() should return list of ObjectVersion."""
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.models import ObjectVersion

        # Upload multiple versions
        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"v1"
        )
        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"v2"
        )

        storage = S3Storage(client=versioned_s3_client)
        versions = storage.list_versions("s3://versioned-bucket/data.txt")

        assert len(versions) == 2
        assert all(isinstance(v, ObjectVersion) for v in versions)

    def test_list_versions_sorted_newest_first(self, versioned_s3_client) -> None:
        """list_versions() should return newest version first."""
        from datacachalog.adapters.storage import S3Storage

        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"v1"
        )
        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"v2"
        )

        storage = S3Storage(client=versioned_s3_client)
        versions = storage.list_versions("s3://versioned-bucket/data.txt")

        # First version should be latest
        assert versions[0].is_latest is True
        # Newer version should come first
        assert versions[0].last_modified >= versions[1].last_modified

    def test_list_versions_with_limit(self, versioned_s3_client) -> None:
        """list_versions() should respect limit parameter."""
        from datacachalog.adapters.storage import S3Storage

        # Upload 5 versions
        for i in range(5):
            versioned_s3_client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=f"v{i}".encode()
            )

        storage = S3Storage(client=versioned_s3_client)
        versions = storage.list_versions("s3://versioned-bucket/data.txt", limit=3)

        assert len(versions) == 3

    def test_list_versions_includes_version_id(self, versioned_s3_client) -> None:
        """list_versions() should include version_id for each version."""
        from datacachalog.adapters.storage import S3Storage

        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"content"
        )

        storage = S3Storage(client=versioned_s3_client)
        versions = storage.list_versions("s3://versioned-bucket/data.txt")

        assert len(versions) == 1
        assert versions[0].version_id is not None

    def test_list_versions_includes_metadata(self, versioned_s3_client) -> None:
        """list_versions() should include etag and size."""
        from datacachalog.adapters.storage import S3Storage

        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"hello"
        )

        storage = S3Storage(client=versioned_s3_client)
        versions = storage.list_versions("s3://versioned-bucket/data.txt")

        assert versions[0].etag is not None
        assert versions[0].size == 5

    def test_list_versions_handles_delete_markers(self, versioned_s3_client) -> None:
        """list_versions() should include delete markers."""
        from datacachalog.adapters.storage import S3Storage

        # Create object and then delete it (creates delete marker)
        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"content"
        )
        versioned_s3_client.delete_object(Bucket="versioned-bucket", Key="data.txt")

        storage = S3Storage(client=versioned_s3_client)
        versions = storage.list_versions("s3://versioned-bucket/data.txt")

        # Should have delete marker and original version
        assert len(versions) == 2
        # One should be a delete marker
        delete_markers = [v for v in versions if v.is_delete_marker]
        assert len(delete_markers) == 1


@pytest.mark.storage
class TestHeadVersion:
    """Tests for head_version() method."""

    def test_head_version_returns_file_metadata(self, versioned_s3_client) -> None:
        """head_version() should return FileMetadata for specific version."""
        from datacachalog.adapters.storage import S3Storage

        # Upload two versions
        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"v1"
        )
        resp = versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"version2"
        )
        version_id = resp["VersionId"]

        storage = S3Storage(client=versioned_s3_client)
        metadata = storage.head_version("s3://versioned-bucket/data.txt", version_id)

        assert isinstance(metadata, FileMetadata)
        assert metadata.etag is not None
        assert metadata.size == 8  # "version2" is 8 bytes

    def test_head_version_not_found_raises_error(self, versioned_s3_client) -> None:
        """head_version() should raise StorageNotFoundError for missing version."""
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.exceptions import StorageNotFoundError

        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"content"
        )

        storage = S3Storage(client=versioned_s3_client)

        with pytest.raises(StorageNotFoundError):
            storage.head_version(
                "s3://versioned-bucket/data.txt", "nonexistent-version-id"
            )


@pytest.mark.storage
class TestDownloadVersion:
    """Tests for download_version() method."""

    def test_download_version_downloads_specific_version(
        self, versioned_s3_client, tmp_path: Path
    ) -> None:
        """download_version() should download specific version content."""
        from datacachalog.adapters.storage import S3Storage

        # Upload two versions
        resp1 = versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"first version"
        )
        v1_id = resp1["VersionId"]
        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"second version"
        )

        storage = S3Storage(client=versioned_s3_client)
        dest = tmp_path / "downloaded.txt"

        # Download the first version (not the latest)
        storage.download_version(
            "s3://versioned-bucket/data.txt",
            dest,
            v1_id,
            progress=lambda x, y: None,
        )

        assert dest.exists()
        assert dest.read_text() == "first version"

    def test_download_version_reports_progress(
        self, versioned_s3_client, tmp_path: Path
    ) -> None:
        """download_version() should call progress callback."""
        from datacachalog.adapters.storage import S3Storage

        resp = versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"x" * 1000
        )
        version_id = resp["VersionId"]

        progress_calls: list[tuple[int, int]] = []

        def track_progress(downloaded: int, total: int) -> None:
            progress_calls.append((downloaded, total))

        storage = S3Storage(client=versioned_s3_client)
        dest = tmp_path / "downloaded.txt"
        storage.download_version(
            "s3://versioned-bucket/data.txt",
            dest,
            version_id,
            progress=track_progress,
        )

        assert len(progress_calls) > 0
        assert progress_calls[-1][0] == 1000

    def test_download_version_not_found_raises_error(
        self, versioned_s3_client, tmp_path: Path
    ) -> None:
        """download_version() should raise StorageNotFoundError for missing version."""
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.exceptions import StorageNotFoundError

        versioned_s3_client.put_object(
            Bucket="versioned-bucket", Key="data.txt", Body=b"content"
        )

        storage = S3Storage(client=versioned_s3_client)
        dest = tmp_path / "downloaded.txt"

        with pytest.raises(StorageNotFoundError):
            storage.download_version(
                "s3://versioned-bucket/data.txt",
                dest,
                "nonexistent-version-id",
                progress=lambda x, y: None,
            )
