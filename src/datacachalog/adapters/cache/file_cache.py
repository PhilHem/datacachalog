"""File-based cache adapter implementing CachePort."""

from __future__ import annotations

import json
import shutil
from datetime import datetime
from pathlib import Path  # noqa: TC003 - used at runtime, not just type hints

from datacachalog.core.exceptions import CacheCorruptError
from datacachalog.core.models import CacheMetadata


class FileCache:
    """Local file cache with JSON metadata sidecars.

    Stores cached files in a directory with accompanying .meta.json
    files containing staleness information.

    Attributes:
        cache_dir: Directory where cached files are stored.
    """

    def __init__(self, cache_dir: Path) -> None:
        """Initialize the cache with a directory path.

        Args:
            cache_dir: Directory where cached files will be stored.
        """
        self.cache_dir = cache_dir

    def _file_path(self, key: str) -> Path:
        """Get the path for a cached file."""
        return self.cache_dir / key

    def _meta_path(self, key: str) -> Path:
        """Get the path for a metadata sidecar file."""
        return self.cache_dir / f"{key}.meta.json"

    def get(self, key: str) -> tuple[Path, CacheMetadata] | None:
        """Get cached file path and metadata, or None if not cached.

        Args:
            key: Cache key identifying the file.

        Returns:
            Tuple of (file path, metadata) if cached, None otherwise.

        Raises:
            CacheCorruptError: If metadata file exists but is corrupt/unreadable.
        """
        file_path = self._file_path(key)
        meta_path = self._meta_path(key)

        if not file_path.exists() or not meta_path.exists():
            return None

        try:
            with meta_path.open() as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            raise CacheCorruptError(
                f"Cache metadata corrupt for '{key}'",
                key=key,
                path=meta_path,
                cause=e,
            ) from e

        metadata = CacheMetadata(
            etag=data.get("etag"),
            last_modified=(
                datetime.fromisoformat(data["last_modified"])
                if data.get("last_modified")
                else None
            ),
            cached_at=datetime.fromisoformat(data["cached_at"]),
            source=data.get("source", ""),
        )

        return file_path, metadata

    def put(self, key: str, path: Path, metadata: CacheMetadata) -> None:
        """Store a file in cache with associated metadata.

        Args:
            key: Cache key for the file.
            path: Path to the source file to cache.
            metadata: Metadata to store alongside the cached file.
        """
        file_path = self._file_path(key)
        meta_path = self._meta_path(key)

        # Create parent directories (handles nested keys like "dataset/file.txt")
        file_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy file to cache
        shutil.copy2(path, file_path)

        # Write metadata sidecar
        data = {
            "etag": metadata.etag,
            "last_modified": (
                metadata.last_modified.isoformat() if metadata.last_modified else None
            ),
            "cached_at": metadata.cached_at.isoformat(),
            "source": metadata.source,
        }
        with meta_path.open("w") as f:
            json.dump(data, f)

    def invalidate(self, key: str) -> None:
        """Remove a file from cache.

        Args:
            key: Cache key to invalidate.
        """
        file_path = self._file_path(key)
        meta_path = self._meta_path(key)

        file_path.unlink(missing_ok=True)
        meta_path.unlink(missing_ok=True)
