"""Filesystem storage adapter for local file operations."""

import hashlib
from datetime import UTC, datetime
from pathlib import Path

from datacachalog.core.exceptions import StorageNotFoundError
from datacachalog.core.models import FileMetadata
from datacachalog.core.ports import ProgressCallback


# Chunk size for reading files (64KB)
_CHUNK_SIZE = 64 * 1024


class FilesystemStorage:
    """Storage adapter for local filesystem operations.

    Implements StoragePort protocol for local file operations.
    Useful for local development and testing without S3.
    """

    def head(self, source: str) -> FileMetadata:
        """Get file metadata without reading full contents.

        Args:
            source: Path to file (absolute or relative).

        Returns:
            FileMetadata with etag (MD5 hash), last_modified, and size.

        Raises:
            StorageNotFoundError: If file does not exist.
        """
        path = Path(source)
        try:
            stat = path.stat()
        except FileNotFoundError as e:
            raise StorageNotFoundError(
                f"File not found: {source}",
                source=source,
                cause=e,
            ) from e

        # Compute MD5 hash for ETag (matches S3 behavior for non-multipart)
        md5_hash = hashlib.md5()
        with path.open("rb") as f:
            for chunk in iter(lambda: f.read(_CHUNK_SIZE), b""):
                md5_hash.update(chunk)

        return FileMetadata(
            etag=f'"{md5_hash.hexdigest()}"',
            last_modified=datetime.fromtimestamp(stat.st_mtime, tz=UTC),
            size=stat.st_size,
        )

    def download(self, source: str, dest: Path, progress: ProgressCallback) -> None:
        """Copy a file from source to destination with progress reporting.

        Args:
            source: Path to source file.
            dest: Destination path.
            progress: Callback function(bytes_downloaded, total_bytes).

        Raises:
            StorageNotFoundError: If source file does not exist.
        """
        source_path = Path(source)
        try:
            total_size = source_path.stat().st_size
        except FileNotFoundError as e:
            raise StorageNotFoundError(
                f"File not found: {source}",
                source=source,
                cause=e,
            ) from e

        bytes_copied = 0

        with source_path.open("rb") as src, dest.open("wb") as dst:
            for chunk in iter(lambda: src.read(_CHUNK_SIZE), b""):
                dst.write(chunk)
                bytes_copied += len(chunk)
                progress(bytes_copied, total_size)

    def upload(
        self, local: Path, dest: str, progress: ProgressCallback | None = None
    ) -> None:
        """Copy a local file to destination path with optional progress reporting.

        Args:
            local: Path to local file.
            dest: Destination path string.
            progress: Optional callback function(bytes_uploaded, total_bytes).

        Raises:
            FileNotFoundError: If local file does not exist.
        """
        dest_path = Path(dest)
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        total_size = local.stat().st_size
        bytes_uploaded = 0

        with local.open("rb") as src, dest_path.open("wb") as dst:
            for chunk in iter(lambda: src.read(_CHUNK_SIZE), b""):
                dst.write(chunk)
                bytes_uploaded += len(chunk)
                if progress:
                    progress(bytes_uploaded, total_size)

    def list(self, prefix: str, pattern: str | None = None) -> list[str]:
        """List files matching a prefix directory and optional glob pattern.

        Args:
            prefix: Directory path to search.
            pattern: Optional glob pattern for filtering (e.g., "*.parquet").
                Supports ** for recursive matching.

        Returns:
            List of full paths for matching files, sorted alphabetically.

        Raises:
            StorageNotFoundError: If the prefix directory does not exist.
        """
        base = Path(prefix)

        if not base.exists():
            raise StorageNotFoundError(
                f"Directory not found: {prefix}",
                source=prefix,
            )

        matches = base.glob(pattern) if pattern else base.iterdir()

        # Filter to files only and sort
        return sorted(str(p) for p in matches if p.is_file())
