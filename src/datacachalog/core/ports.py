"""Port interfaces for hexagonal architecture.

Ports define contracts that adapters must implement. The core domain
depends only on these protocols, never on concrete implementations.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable


if TYPE_CHECKING:
    import builtins
    from concurrent.futures import Future
    from pathlib import Path

    from datacachalog.core.models import CacheMetadata, FileMetadata, ObjectVersion

ProgressCallback = Callable[[int, int], None]


@runtime_checkable
class StoragePort(Protocol):
    """Remote storage backend (S3, local filesystem)."""

    def download(self, source: str, dest: Path, progress: ProgressCallback) -> None:
        """Download a file from remote storage to local path."""
        ...

    def upload(
        self, local: Path, dest: str, progress: ProgressCallback | None = None
    ) -> None:
        """Upload a local file to remote storage with optional progress.

        Args:
            local: Path to local file.
            dest: Remote destination URI.
            progress: Optional callback function(bytes_uploaded, total_bytes).
        """
        ...

    def head(self, source: str) -> FileMetadata:
        """Get file metadata (ETag, LastModified) without downloading."""
        ...

    def list(self, prefix: str, pattern: str | None = None) -> list[str]:
        """List files matching a prefix and optional glob pattern.

        Args:
            prefix: Base URI/path to search (e.g., "s3://bucket/logs/" or "/data/").
            pattern: Optional glob pattern for filtering (e.g., "*.parquet").
                If None, returns all files under prefix.

        Returns:
            List of full URIs/paths for matching files, sorted alphabetically.

        Raises:
            StorageNotFoundError: If the prefix path does not exist.
        """
        ...

    def list_versions(
        self, source: str, limit: int | None = None
    ) -> builtins.list[ObjectVersion]:
        """List all versions of an object, newest first.

        Args:
            source: Remote storage URI (e.g., "s3://bucket/path/to/file.parquet").
            limit: Maximum number of versions to return. If None, returns all.

        Returns:
            List of ObjectVersion sorted by last_modified descending (newest first).

        Raises:
            VersioningNotSupportedError: If storage doesn't support versioning.
            StorageNotFoundError: If the object has never existed.
        """
        ...

    def head_version(self, source: str, version_id: str) -> FileMetadata:
        """Get metadata for a specific version of an object.

        Args:
            source: Remote storage URI.
            version_id: The version identifier.

        Returns:
            FileMetadata for the specified version.

        Raises:
            VersioningNotSupportedError: If storage doesn't support versioning.
            StorageNotFoundError: If the version does not exist.
        """
        ...

    def download_version(
        self,
        source: str,
        dest: Path,
        version_id: str,
        progress: ProgressCallback,
    ) -> None:
        """Download a specific version of an object.

        Args:
            source: Remote storage URI.
            dest: Local path to download to.
            version_id: The version identifier.
            progress: Callback function(bytes_downloaded, total_bytes).

        Raises:
            VersioningNotSupportedError: If storage doesn't support versioning.
            StorageNotFoundError: If the version does not exist.
        """
        ...


@runtime_checkable
class CachePort(Protocol):
    """Local file cache with metadata tracking."""

    def get(self, key: str) -> tuple[Path, CacheMetadata] | None:
        """Get cached file path and metadata, or None if not cached."""
        ...

    def put(self, key: str, path: Path, metadata: CacheMetadata) -> None:
        """Store a file in cache with associated metadata."""
        ...

    def invalidate(self, key: str) -> None:
        """Remove a file from cache."""
        ...

    def invalidate_prefix(self, prefix: str) -> int:
        """Remove all cache entries with keys starting with prefix.

        Used to invalidate all cached files for a glob pattern dataset.
        For example, invalidate_prefix("logs") removes all entries like
        "logs/2024-01.parquet", "logs/2024-02.parquet", etc.

        Args:
            prefix: The key prefix to match (typically the dataset name).

        Returns:
            Number of entries removed.
        """
        ...

    def list_all_keys(self) -> builtins.list[str]:
        """List all cache keys.

        Returns:
            List of all keys currently in the cache.
        """
        ...


@runtime_checkable
class ProgressReporter(Protocol):
    """Reports download progress to the user.

    This protocol defines the contract for progress display adapters.
    The core domain uses this to report progress without depending
    on any specific UI library.
    """

    def start_task(self, name: str, total: int) -> ProgressCallback:
        """Start tracking a download task.

        Args:
            name: Human-readable name for the task (dataset name).
            total: Total bytes to download.

        Returns:
            A ProgressCallback to call with (bytes_downloaded, total_bytes).
        """
        ...

    def finish_task(self, name: str) -> None:
        """Mark a task as complete.

        Args:
            name: The task name passed to start_task().
        """
        ...


class NullProgressReporter:
    """A ProgressReporter that produces no output.

    Used as the default when no progress reporting is desired.
    """

    def start_task(self, name: str, total: int) -> ProgressCallback:  # noqa: ARG002
        """Return a no-op callback."""
        return lambda _downloaded, _total: None

    def finish_task(self, name: str) -> None:
        """Do nothing."""
        _ = name  # Unused but required by protocol


@runtime_checkable
class ExecutorPort(Protocol):
    """Executor for parallel task execution.

    Abstracts over concurrent.futures executors to allow dependency injection
    and testing. The core domain uses this protocol instead of directly
    importing ThreadPoolExecutor, maintaining "concurrency at the edges".
    """

    def submit(
        self, fn: Callable[..., object], *args: object, **kwargs: object
    ) -> Future[object]:  # type: ignore[name-defined, unused-ignore]
        """Submit a function for execution.

        Args:
            fn: Function to execute.
            *args: Positional arguments to pass to fn.
            **kwargs: Keyword arguments to pass to fn.

        Returns:
            Future representing the pending result.
        """
        ...

    def __enter__(self) -> ExecutorPort:
        """Enter context manager."""
        ...

    def __exit__(
        self, exc_type: object, exc_val: object, exc_tb: object
    ) -> object | None:
        """Exit context manager."""
        ...
