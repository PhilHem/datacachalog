"""Domain exceptions for datacachalog.

All library errors inherit from DatacachalogError, allowing users to catch
any library exception with a single except clause. Each exception provides
a recovery_hint property with guidance on resolving the error.
"""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from pathlib import Path


class DatacachalogError(Exception):
    """Base class for all datacachalog exceptions.

    Catch this to handle any error from the library.
    """

    @property
    def recovery_hint(self) -> str | None:
        """Optional guidance on how to resolve this error."""
        return None


class DatasetNotFoundError(DatacachalogError):
    """Raised when a requested dataset doesn't exist in the catalog.

    Attributes:
        name: The dataset name that was not found.
        available: List of available dataset names.
    """

    def __init__(self, name: str, available: list[str] | None = None) -> None:
        self.name = name
        self.available = available if available is not None else []
        super().__init__(f"Dataset '{name}' not found")

    @property
    def recovery_hint(self) -> str:
        """Suggest available datasets or how to check them."""
        if self.available:
            return f"Available datasets: {', '.join(self.available)}"
        return "Check catalog.datasets for available names"


class StorageError(DatacachalogError):
    """Base class for storage-related errors.

    Raised when remote storage operations (S3, filesystem) fail.

    Attributes:
        source: The storage path/URI that caused the error.
        cause: The underlying exception, if any.
    """

    def __init__(
        self,
        message: str,
        source: str,
        cause: Exception | None = None,
    ) -> None:
        self.source = source
        self.cause = cause
        super().__init__(message)


class StorageNotFoundError(StorageError):
    """Raised when the requested file/object doesn't exist in storage."""

    @property
    def recovery_hint(self) -> str:
        """Suggest verifying the path."""
        return f"Verify the source path exists: {self.source}"


class StorageAccessError(StorageError):
    """Raised when access is denied to storage (permissions, credentials)."""

    @property
    def recovery_hint(self) -> str:
        """Suggest checking permissions."""
        return "Check credentials and bucket/path permissions"


class CacheError(DatacachalogError):
    """Base class for cache-related errors."""

    pass


class CacheCorruptError(CacheError):
    """Raised when cache metadata is corrupt or unreadable.

    Attributes:
        key: The cache key for the corrupt entry.
        path: The path to the corrupt file.
    """

    def __init__(
        self,
        message: str,
        key: str,
        path: Path,
        cause: Exception | None = None,
    ) -> None:
        self.key = key
        self.path = path
        self.cause = cause
        super().__init__(message)

    @property
    def recovery_hint(self) -> str:
        """Suggest deleting the corrupt cache entry."""
        return f"Delete cache files for '{self.key}' and re-fetch"


class ConfigurationError(DatacachalogError):
    """Raised for configuration problems (missing required settings)."""

    pass


class CatalogLoadError(DatacachalogError):
    """Raised when a catalog file cannot be loaded.

    Attributes:
        catalog_path: Path to the catalog file that failed to load.
        line: Line number where the error occurred (if available).
        cause: The underlying exception, if any.
    """

    def __init__(
        self,
        message: str,
        catalog_path: Path,
        line: int | None = None,
        cause: Exception | None = None,
    ) -> None:
        self.catalog_path = catalog_path
        self.line = line
        self.cause = cause
        super().__init__(message)

    @property
    def recovery_hint(self) -> str:
        """Suggest checking the catalog file at the specific line."""
        if self.line:
            return f"Check {self.catalog_path.name} at line {self.line}"
        return f"Check {self.catalog_path.name} for syntax or import errors"
