"""S3 storage adapter using boto3."""

from __future__ import annotations

from typing import TYPE_CHECKING

import boto3

from datacachalog.core.models import FileMetadata


if TYPE_CHECKING:
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
            botocore.exceptions.ClientError: If object does not exist.
        """
        bucket, key = self._parse_s3_uri(source)
        response = self._client.head_object(Bucket=bucket, Key=key)

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
            botocore.exceptions.ClientError: If object does not exist.
        """
        bucket, key = self._parse_s3_uri(source)

        # Get object with streaming body
        response = self._client.get_object(Bucket=bucket, Key=key)
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
