"""Fetch operation implementations for Catalog.

This module contains the internal fetch logic that Catalog delegates to.
These are implementation details and should not be used directly.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from datacachalog.core.exceptions import ConfigurationError, EmptyGlobMatchError
from datacachalog.core.glob_utils import derive_cache_key, split_glob_pattern
from datacachalog.core.models import CacheMetadata, Dataset


if TYPE_CHECKING:
    from datacachalog.core.ports import CachePort, ProgressReporter, StoragePort

# Type aliases for the callable parameters
ResolveCachePath = Callable[[Dataset], Path]
ResolveVersionCacheKey = Callable[[Dataset, datetime], str]


def fetch_single(
    cache_key: str,
    dataset: Dataset,
    progress: ProgressReporter,
    storage: StoragePort,
    cache: CachePort,
    resolve_cache_path: ResolveCachePath,
    *,
    dry_run: bool = False,
) -> Path:
    """Fetch a single file with caching and staleness detection."""
    # Check cache
    cached = cache.get(cache_key)
    if cached is not None:
        cached_path, cache_meta = cached
        remote_meta = storage.head(dataset.source)
        if not cache_meta.is_stale(remote_meta):
            return cached_path

    # In dry-run mode, check staleness but don't download or update cache
    if dry_run:
        storage.head(dataset.source)  # Check remote exists
        if cached is not None:
            return cached_path
        return resolve_cache_path(dataset)

    # Cache miss or stale - download with progress
    dest = resolve_cache_path(dataset)
    dest.parent.mkdir(parents=True, exist_ok=True)

    remote_meta = storage.head(dataset.source)
    total_size = remote_meta.size or 0

    callback = progress.start_task(cache_key, total_size)
    try:
        storage.download(dataset.source, dest, callback)
    finally:
        progress.finish_task(cache_key)

    # Store in cache with metadata
    cache_meta = CacheMetadata(
        etag=remote_meta.etag,
        last_modified=remote_meta.last_modified,
        source=dataset.source,
    )
    cache.put(cache_key, dest, cache_meta)

    # Return the cache's path (may differ from dest due to cache internals)
    cached_result = cache.get(cache_key)
    if cached_result is None:
        return dest
    return cached_result[0]


def fetch_version(
    dataset: Dataset,
    version_id: str,
    progress: ProgressReporter,
    storage: StoragePort,
    cache: CachePort,
    cache_dir: Path | None,
    resolve_version_cache_key: ResolveVersionCacheKey,
    *,
    dry_run: bool = False,
) -> Path:
    """Fetch a specific version of a dataset."""
    # Get version metadata to generate date-based cache key
    remote_meta = storage.head_version(dataset.source, version_id)

    if remote_meta.last_modified is None:
        raise ValueError(f"Version {version_id} has no last_modified timestamp")

    # Generate date-based cache key
    cache_key = resolve_version_cache_key(dataset, remote_meta.last_modified)

    # Check cache for this specific version
    cached = cache.get(cache_key)
    if cached is not None:
        return cached[0]

    # In dry-run mode, return expected cache path without downloading
    if dry_run:
        if cache_dir is None:
            raise ConfigurationError("cache_dir is required for versioned fetches")
        return cache_dir / cache_key

    # Download to a temporary location first
    import tempfile

    if cache_dir is None:
        raise ConfigurationError("cache_dir is required for versioned fetches")
    cache_dir.mkdir(parents=True, exist_ok=True)

    with tempfile.NamedTemporaryFile(delete=False, dir=cache_dir) as tmp_file:
        tmp_path = Path(tmp_file.name)

    try:
        total_size = remote_meta.size or 0
        callback = progress.start_task(cache_key, total_size)
        try:
            storage.download_version(dataset.source, tmp_path, version_id, callback)
        finally:
            progress.finish_task(cache_key)

        cache_meta = CacheMetadata(
            etag=remote_meta.etag,
            last_modified=remote_meta.last_modified,
            source=dataset.source,
        )
        cache.put(cache_key, tmp_path, cache_meta)
    finally:
        tmp_path.unlink(missing_ok=True)

    # Return the cache's path
    cached_result = cache.get(cache_key)
    if cached_result is None:
        return cache_dir / cache_key
    return cached_result[0]


def fetch_glob(
    dataset: Dataset,
    progress: ProgressReporter,
    storage: StoragePort,
    cache: CachePort,
    resolve_cache_path: ResolveCachePath,
    *,
    dry_run: bool = False,
) -> list[Path]:
    """Fetch all files matching a glob pattern."""
    prefix, pattern = split_glob_pattern(dataset.source)
    matched_uris = storage.list(prefix, pattern)

    if not matched_uris:
        raise EmptyGlobMatchError(pattern, prefix)

    paths: list[Path] = []
    for uri in matched_uris:
        cache_key = derive_cache_key(dataset.name, prefix, uri)
        single_dataset = Dataset(
            name=cache_key,
            source=uri,
            description=dataset.description,
        )
        path = fetch_single(
            cache_key,
            single_dataset,
            progress,
            storage,
            cache,
            resolve_cache_path,
            dry_run=dry_run,
        )
        paths.append(path)

    return paths
