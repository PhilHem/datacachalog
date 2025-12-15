"""S3 storage adapter using boto3."""

from __future__ import annotations

import fnmatch
from pathlib import PurePosixPath
from typing import TYPE_CHECKING

import boto3
from botocore.exceptions import ClientError

from datacachalog.core.exceptions import (
    StorageAccessError,
    StorageError,
    StorageNotFoundError,
)
from datacachalog.core.models import FileMetadata, ObjectVersion


if TYPE_CHECKING:
    import builtins
    from pathlib import Path

    from mypy_boto3_s3 import S3Client

    from datacachalog.core.ports import ProgressCallback


# Chunk size for streaming downloads (64KB)
_CHUNK_SIZE = 64 * 1024


class S3Storage:
    """Storage adapter for S3 operations.

    Implements StoragePort protocol for AWS S3.
    """

    def __init__(self, client: S3Client | None = None) -> None:
        """Initialize S3 storage.

        Args:
            client: Optional boto3 S3 client. If not provided, creates a default client.
        """
        self._client = client or boto3.client("s3")

    def head(self, source: str) -> FileMetadata:
        """Get file metadata without downloading.

        Args:
            source: S3 URI (s3://bucket/key).

        Returns:
            FileMetadata with etag, last_modified, and size.

        Raises:
            StorageNotFoundError: If object does not exist.
            StorageAccessError: If access is denied.
            StorageError: For other S3 errors.
        """
        bucket, key = self._parse_s3_uri(source)
        try:
            response = self._client.head_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise self._translate_client_error(e, source) from e

        return FileMetadata(
            etag=response["ETag"],
            last_modified=response["LastModified"],
            size=response["ContentLength"],
        )

    def download(self, source: str, dest: Path, progress: ProgressCallback) -> None:
        """Download a file from S3 to local path with progress reporting.

        Args:
            source: S3 URI (s3://bucket/key).
            dest: Local destination path.
            progress: Callback function(bytes_downloaded, total_bytes).

        Raises:
            StorageNotFoundError: If object does not exist.
            StorageAccessError: If access is denied.
            StorageError: For other S3 errors.
        """
        bucket, key = self._parse_s3_uri(source)

        try:
            # Get object with streaming body
            response = self._client.get_object(Bucket=bucket, Key=key)
        except ClientError as e:
            raise self._translate_client_error(e, source) from e

        total_size = response["ContentLength"]
        body = response["Body"]

        bytes_downloaded = 0
        with dest.open("wb") as f:
            for chunk in iter(lambda: body.read(_CHUNK_SIZE), b""):
                f.write(chunk)
                bytes_downloaded += len(chunk)
                progress(bytes_downloaded, total_size)

    def upload(
        self, local: Path, dest: str, progress: ProgressCallback | None = None
    ) -> None:
        """Upload a local file to S3 with optional progress reporting.

        Args:
            local: Path to local file.
            dest: S3 URI (s3://bucket/key).
            progress: Optional callback function(bytes_uploaded, total_bytes).

        Raises:
            FileNotFoundError: If local file does not exist.
        """
        bucket, key = self._parse_s3_uri(dest)
        total_size = local.stat().st_size
        bytes_uploaded = 0

        # Read file in chunks, tracking progress
        chunks: list[bytes] = []
        with local.open("rb") as f:
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                chunks.append(chunk)
                bytes_uploaded += len(chunk)
                if progress:
                    progress(bytes_uploaded, total_size)

        # Upload complete file
        self._client.put_object(Bucket=bucket, Key=key, Body=b"".join(chunks))

    def list(self, prefix: str, pattern: str | None = None) -> list[str]:
        """List S3 objects matching a prefix and optional glob pattern.

        Args:
            prefix: S3 URI prefix (e.g., "s3://bucket/path/").
            pattern: Optional glob pattern for filtering (e.g., "*.parquet").
                Supports ** for matching any depth.

        Returns:
            List of full S3 URIs for matching objects, sorted alphabetically.
        """
        bucket, key_prefix = self._parse_s3_uri_prefix(prefix)

        paginator = self._client.get_paginator("list_objects_v2")
        results: list[str] = []

        for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]

                if pattern is None:
                    results.append(f"s3://{bucket}/{key}")
                else:
                    filename = PurePosixPath(key).name

                    # Handle ** pattern (match any depth)
                    if "**" in pattern:
                        # For **/*.ext, match files at any depth including root
                        # Extract the file pattern (e.g., "*.parquet" from "**/*.parquet")
                        file_pattern = pattern.split("/")[-1]
                        if fnmatch.fnmatch(filename, file_pattern):
                            results.append(f"s3://{bucket}/{key}")
                    else:
                        # For simple patterns, match just the filename
                        if fnmatch.fnmatch(filename, pattern):
                            results.append(f"s3://{bucket}/{key}")

        return sorted(results)

    def list_versions(
        self, source: str, limit: int | None = None
    ) -> builtins.list[ObjectVersion]:
        """List all versions of an S3 object, newest first.

        Args:
            source: S3 URI (s3://bucket/key).
            limit: Maximum number of versions to return. If None, returns all.

        Returns:
            List of ObjectVersion sorted by last_modified descending (newest first).
        """
        bucket, key = self._parse_s3_uri(source)

        versions: builtins.list[ObjectVersion] = []
        paginator = self._client.get_paginator("list_object_versions")

        for page in paginator.paginate(Bucket=bucket, Prefix=key):
            # Process regular versions
            for v in page.get("Versions", []):
                if v["Key"] == key:  # Exact match only
                    versions.append(
                        ObjectVersion(
                            version_id=v["VersionId"],
                            last_modified=v["LastModified"],
                            etag=v["ETag"],
                            size=v["Size"],
                            is_latest=v["IsLatest"],
                            is_delete_marker=False,
                        )
                    )

            # Process delete markers
            for dm in page.get("DeleteMarkers", []):
                if dm["Key"] == key:  # Exact match only
                    versions.append(
                        ObjectVersion(
                            version_id=dm["VersionId"],
                            last_modified=dm["LastModified"],
                            is_latest=dm["IsLatest"],
                            is_delete_marker=True,
                        )
                    )

        # Sort newest first and apply limit
        versions.sort(reverse=True)
        return versions[:limit] if limit else versions

    def head_version(self, source: str, version_id: str) -> FileMetadata:
        """Get metadata for a specific version of an S3 object.

        Args:
            source: S3 URI (s3://bucket/key).
            version_id: The version identifier.

        Returns:
            FileMetadata for the specified version.

        Raises:
            StorageNotFoundError: If the version does not exist.
            StorageAccessError: If access is denied.
            StorageError: For other S3 errors.
        """
        bucket, key = self._parse_s3_uri(source)
        try:
            response = self._client.head_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )
        except ClientError as e:
            raise self._translate_client_error(e, source) from e

        return FileMetadata(
            etag=response["ETag"],
            last_modified=response["LastModified"],
            size=response["ContentLength"],
        )

    def download_version(
        self,
        source: str,
        dest: Path,
        version_id: str,
        progress: ProgressCallback,
    ) -> None:
        """Download a specific version of an S3 object.

        Args:
            source: S3 URI (s3://bucket/key).
            dest: Local destination path.
            version_id: The version identifier.
            progress: Callback function(bytes_downloaded, total_bytes).

        Raises:
            StorageNotFoundError: If the version does not exist.
            StorageAccessError: If access is denied.
            StorageError: For other S3 errors.
        """
        bucket, key = self._parse_s3_uri(source)

        try:
            response = self._client.get_object(
                Bucket=bucket, Key=key, VersionId=version_id
            )
        except ClientError as e:
            raise self._translate_client_error(e, source) from e

        total_size = response["ContentLength"]
        body = response["Body"]

        bytes_downloaded = 0
        with dest.open("wb") as f:
            for chunk in iter(lambda: body.read(_CHUNK_SIZE), b""):
                f.write(chunk)
                bytes_downloaded += len(chunk)
                progress(bytes_downloaded, total_size)

    def _parse_s3_uri_prefix(self, uri: str) -> tuple[str, str]:
        """Parse an S3 URI prefix into bucket and key prefix.

        Unlike _parse_s3_uri, this allows empty key (bucket-level prefix).

        Args:
            uri: S3 URI in format s3://bucket/ or s3://bucket/prefix/.

        Returns:
            Tuple of (bucket, key_prefix).

        Raises:
            ValueError: If URI is not a valid S3 URI.
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {uri}")

        path = uri[5:]  # Remove s3://
        parts = path.split("/", 1)
        bucket = parts[0]
        key_prefix = parts[1] if len(parts) > 1 else ""

        return bucket, key_prefix

    def _parse_s3_uri(self, uri: str) -> tuple[str, str]:
        """Parse an S3 URI into bucket and key.

        Args:
            uri: S3 URI in format s3://bucket/key.

        Returns:
            Tuple of (bucket, key).

        Raises:
            ValueError: If URI is not a valid S3 URI.
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"Invalid S3 URI: {uri}")

        # Remove s3:// prefix
        path = uri[5:]

        # Split on first /
        parts = path.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid S3 URI (missing key): {uri}")

        bucket, key = parts
        return bucket, key

    def _translate_client_error(self, error: ClientError, source: str) -> StorageError:
        """Translate botocore ClientError to domain exception.

        Args:
            error: The botocore ClientError.
            source: The source URI for context.

        Returns:
            Appropriate StorageError subclass.
        """
        code = error.response.get("Error", {}).get("Code", "")

        # Not found errors
        if code in ("404", "NoSuchKey", "NoSuchBucket", "NoSuchVersion"):
            return StorageNotFoundError(
                f"Object not found: {source}",
                source=source,
                cause=error,
            )

        # Access denied errors
        if code in ("403", "AccessDenied"):
            return StorageAccessError(
                f"Access denied: {source}",
                source=source,
                cause=error,
            )

        # Generic S3 error
        return StorageError(
            f"S3 error ({code}): {error}",
            source=source,
            cause=error,
        )
