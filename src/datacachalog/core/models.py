"""Core domain models for datacachalog.

These models are pure Python dataclasses with no I/O dependencies.
They represent the core domain concepts of the data catalog.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Self


if TYPE_CHECKING:
    from datacachalog.core.ports import Reader


@dataclass(frozen=True, slots=True)
class Dataset:
    """A named dataset pointing to a remote storage location.

    Attributes:
        name: Unique identifier for the dataset within a catalog.
        source: Remote storage URI (e.g., "s3://bucket/path/to/file.parquet").
        description: Optional human-readable description of the dataset.
        cache_path: Optional explicit local path for caching. If not provided,
            the catalog will derive a path from the source URI and cache_dir.
        reader: Optional reader to load cached files into typed objects (e.g.,
            pandas DataFrame). If not provided, fetch() returns Path only.

    Example:
        >>> customers = Dataset(
        ...     name="customers",
        ...     source="s3://bucket/customers/data.parquet",
        ...     description="Customer master data",
        ... )
        >>> customers.name
        'customers'
    """

    name: str
    source: str
    description: str = ""
    cache_path: Path | None = None
    reader: Reader[Any] | None = None

    def __post_init__(self) -> None:
        """Validate dataset fields after initialization."""
        if not self.name:
            raise ValueError("Dataset name cannot be empty")
        if not self.source:
            raise ValueError("Dataset source cannot be empty")

    def with_cache_path(self, cache_path: Path) -> Self:
        """Return a new Dataset with the specified cache path.

        Args:
            cache_path: The local path where the dataset should be cached.

        Returns:
            A new Dataset instance with the updated cache_path.
        """
        return type(self)(
            name=self.name,
            source=self.source,
            description=self.description,
            cache_path=cache_path,
            reader=self.reader,
        )

    def with_resolved_paths(self, root: Path) -> Self:
        """Return new Dataset with cache_path resolved against root.

        Relative cache_path values are joined with root to produce
        absolute paths. Absolute paths and None are unchanged.

        Args:
            root: Project root directory to resolve relative paths against.

        Returns:
            New Dataset with resolved cache_path.
        """
        if self.cache_path is None or self.cache_path.is_absolute():
            return self

        resolved = root / self.cache_path
        return type(self)(
            name=self.name,
            source=self.source,
            description=self.description,
            cache_path=resolved,
            reader=self.reader,
        )


@dataclass(frozen=True, slots=True)
class FileMetadata:
    """Metadata about a remote file, returned by storage head() operations.

    This represents the "freshness" indicators used to detect staleness.
    Either etag or last_modified (or both) should be present.

    Attributes:
        etag: HTTP ETag header value (e.g., S3 object ETag).
        last_modified: Last modification timestamp of the remote file.
        size: File size in bytes (optional, for progress display).
    """

    etag: str | None = None
    last_modified: datetime | None = None
    size: int | None = None

    def __post_init__(self) -> None:
        """Validate that at least one staleness indicator is present."""
        if self.etag is None and self.last_modified is None:
            raise ValueError("FileMetadata must have at least etag or last_modified")

    def matches(self, other: FileMetadata) -> bool:
        """Check if this metadata matches another for staleness detection.

        Two FileMetadata objects match if they have the same ETag (when both
        have ETags) or the same last_modified timestamp (when both have
        timestamps). ETag comparison takes precedence.

        Args:
            other: Another FileMetadata to compare against.

        Returns:
            True if the metadata indicates the same file version.
        """
        # ETag comparison takes precedence if both have ETags
        if self.etag is not None and other.etag is not None:
            return self.etag == other.etag

        # Fall back to last_modified comparison
        if self.last_modified is not None and other.last_modified is not None:
            return self.last_modified == other.last_modified

        # Cannot determine match if no common fields
        return False


@dataclass(frozen=True, slots=True)
class CacheMetadata:
    """Metadata stored alongside cached files for staleness detection.

    This is persisted as a JSON sidecar file next to the cached data file.
    It records the remote file's freshness indicators at the time of download.

    Attributes:
        etag: ETag of the remote file when it was cached.
        last_modified: Last modification time of the remote file when cached.
        cached_at: Timestamp when the file was cached locally.
        source: Original source URI (for verification).
    """

    etag: str | None = None
    last_modified: datetime | None = None
    cached_at: datetime = field(default_factory=datetime.now)
    source: str = ""

    def to_file_metadata(self) -> FileMetadata:
        """Convert to FileMetadata for staleness comparison.

        Returns:
            A FileMetadata with the cached etag and last_modified values.

        Raises:
            ValueError: If neither etag nor last_modified is available.
        """
        return FileMetadata(
            etag=self.etag,
            last_modified=self.last_modified,
        )

    def is_stale(self, remote: FileMetadata) -> bool:
        """Check if the cached version is stale compared to remote.

        Args:
            remote: Current metadata from the remote storage.

        Returns:
            True if the cache is stale and should be re-downloaded.
        """
        return not self.to_file_metadata().matches(remote)


@dataclass(frozen=True, slots=True)
class ObjectVersion:
    """Metadata for a versioned object in storage.

    Represents a single version of an object, as returned by S3's
    ListObjectVersions API. Used for time-travel queries and version listing.

    Attributes:
        last_modified: When this version was created.
        version_id: S3 version ID (None for non-versioned buckets).
        etag: Object ETag for this version.
        size: Object size in bytes.
        is_latest: Whether this is the current/latest version.
        is_delete_marker: Whether this version is a delete marker.
    """

    last_modified: datetime
    version_id: str | None = None
    etag: str | None = None
    size: int | None = None
    is_latest: bool = False
    is_delete_marker: bool = False

    def __lt__(self, other: Self) -> bool:
        """Compare versions by last_modified for sorting."""
        return self.last_modified < other.last_modified

    def to_file_metadata(self) -> FileMetadata:
        """Convert to FileMetadata for staleness comparison.

        Returns:
            A FileMetadata with the version's etag, last_modified, and size.
        """
        return FileMetadata(
            etag=self.etag,
            last_modified=self.last_modified,
            size=self.size,
        )


def find_version_at(
    versions: list[ObjectVersion],
    as_of: datetime,
) -> ObjectVersion | None:
    """Find the version with last_modified closest to and <= as_of.

    This is the core time-travel resolution logic. It finds the version
    that was current at the specified point in time.

    Args:
        versions: List of versions, assumed sorted newest-first.
        as_of: The point in time to resolve to.

    Returns:
        The ObjectVersion active at as_of, or None if no version existed then.

    Note:
        Delete markers are automatically skipped since they represent
        deleted objects that cannot be downloaded.
    """
    for version in versions:
        if version.is_delete_marker:
            continue
        if version.last_modified <= as_of:
            return version
    return None
