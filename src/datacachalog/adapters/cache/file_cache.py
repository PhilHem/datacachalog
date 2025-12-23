"""File-based cache adapter implementing CachePort."""

from __future__ import annotations

import contextlib
import json
import shutil
from datetime import datetime
from pathlib import Path

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
        prefix_dir = self.cache_dir / prefix
        if not prefix_dir.exists():
            return 0

        # Find all metadata files under the prefix directory
        meta_files = list(prefix_dir.rglob("*.meta.json"))
        count = 0

        for meta_path in meta_files:
            # The data file path is the meta path without ".meta.json" suffix
            meta_str = str(meta_path)
            if meta_str.endswith(".meta.json"):
                data_path = Path(meta_str[:-10])  # Remove ".meta.json" (10 chars)
            else:
                continue  # Skip non-meta files

            data_path.unlink(missing_ok=True)
            meta_path.unlink(missing_ok=True)
            count += 1

        # Clean up empty directories
        self._cleanup_empty_dirs(prefix_dir)

        return count

    def _cleanup_empty_dirs(self, path: Path) -> None:
        """Remove empty directories recursively up to cache_dir."""
        try:
            while path != self.cache_dir and path.is_dir():
                if any(path.iterdir()):
                    break  # Directory not empty
                path.rmdir()
                path = path.parent
        except OSError:
            pass  # Directory not empty or other issue, ignore

    def size(self) -> int:
        """Calculate total cache size in bytes.

        Includes both data files and metadata (.meta.json) files.

        Returns:
            Total size in bytes.
        """
        total_size = 0
        if not self.cache_dir.exists():
            return 0

        for file_path in self.cache_dir.rglob("*"):
            if file_path.is_file():
                with contextlib.suppress(OSError):
                    total_size += file_path.stat().st_size

        return total_size

    def statistics(self) -> dict[str, int]:
        """Get cache statistics.

        Returns:
            Dictionary with 'total_size' (bytes) and 'file_count' (number of files).
        """
        total_size = 0
        file_count = 0

        if not self.cache_dir.exists():
            return {"total_size": 0, "file_count": 0}

        for file_path in self.cache_dir.rglob("*"):
            if file_path.is_file():
                with contextlib.suppress(OSError):
                    total_size += file_path.stat().st_size
                    file_count += 1

        return {"total_size": total_size, "file_count": file_count}
