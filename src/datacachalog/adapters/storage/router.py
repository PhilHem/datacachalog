"""RouterStorage composite adapter for URI scheme-based routing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    import builtins
    from pathlib import Path

    from datacachalog.core.models import FileMetadata, ObjectVersion
    from datacachalog.core.ports import ProgressCallback, StoragePort


def parse_uri_scheme(uri: str) -> str | None:
    """Extract the URI scheme from a source string.

    Args:
        uri: Source URI or file path.

    Returns:
        The scheme (e.g., 's3', 'file') or None for local paths.
    """
    if "://" in uri:
        scheme = uri.split("://", 1)[0]
        # Avoid confusing Windows drive letters (C:) with schemes
        if len(scheme) > 1:
            return scheme.lower()
    return None


def strip_file_scheme(uri: str) -> str:
    """Strip file:// prefix from URI, returning plain path.

    Args:
        uri: URI that may have file:// prefix.

    Returns:
        The path without file:// prefix.
    """
    if uri.startswith("file://"):
        return uri[7:]  # len("file://") == 7
    return uri


class RouterStorage:
    """Storage adapter that routes to backends based on URI scheme.

    Implements StoragePort by delegating to scheme-specific adapters.
    """

    def __init__(self, backends: dict[str | None, StoragePort]) -> None:
        """Initialize with scheme-to-adapter mapping.

        Args:
            backends: Mapping of scheme (e.g., 's3', 'file') to StoragePort adapter.
                      Use None as key for default (local paths without scheme).
        """
        self._backends = backends

    def _get_backend_and_path(self, uri: str) -> tuple[StoragePort, str]:
        """Get the appropriate backend and normalized path for a URI."""
        scheme = parse_uri_scheme(uri)
        if scheme in self._backends:
            # Strip file:// prefix for filesystem backend
            path = strip_file_scheme(uri) if scheme == "file" else uri
            return self._backends[scheme], path
        if scheme is None and None in self._backends:
            return self._backends[None], uri
        scheme_display = f"'{scheme}'" if scheme else "local path"
        raise ValueError(f"No storage backend registered for scheme {scheme_display}")

    def head(self, source: str) -> FileMetadata:
        """Get file metadata by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(source)
        return backend.head(path)

    def download(self, source: str, dest: Path, progress: ProgressCallback) -> None:
        """Download file by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(source)
        backend.download(path, dest, progress)

    def upload(
        self, local: Path, dest: str, progress: ProgressCallback | None = None
    ) -> None:
        """Upload file by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(dest)
        backend.upload(local, path, progress)

    def list(self, prefix: str, pattern: str | None = None) -> list[str]:
        """List files by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(prefix)
        return backend.list(path, pattern)

    def list_versions(
        self, source: str, limit: int | None = None
    ) -> builtins.list[ObjectVersion]:
        """List versions by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(source)
        return backend.list_versions(path, limit)

    def head_version(self, source: str, version_id: str) -> FileMetadata:
        """Get version metadata by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(source)
        return backend.head_version(path, version_id)

    def download_version(
        self,
        source: str,
        dest: Path,
        version_id: str,
        progress: ProgressCallback,
    ) -> None:
        """Download specific version by delegating to appropriate backend."""
        backend, path = self._get_backend_and_path(source)
        backend.download_version(path, dest, version_id, progress)


def create_router(s3_client: Any | None = None) -> RouterStorage:
    """Create a RouterStorage with default backends.

    Args:
        s3_client: Optional boto3 S3 client. If not provided, creates default.

    Returns:
        RouterStorage configured with S3Storage and FilesystemStorage.
    """
    from datacachalog.adapters.storage import FilesystemStorage, S3Storage

    fs = FilesystemStorage()
    return RouterStorage(
        backends={
            "s3": S3Storage(client=s3_client),
            "file": fs,
            None: fs,
        }
    )
