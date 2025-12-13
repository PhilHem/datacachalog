"""Port interfaces for hexagonal architecture.

Ports define contracts that adapters must implement. The core domain
depends only on these protocols, never on concrete implementations.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Protocol, runtime_checkable


if TYPE_CHECKING:
    from pathlib import Path

    from datacachalog.core.models import CacheMetadata, FileMetadata

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
