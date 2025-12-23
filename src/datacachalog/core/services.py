"""Core domain services for datacachalog."""

from datetime import datetime
from pathlib import Path

from datacachalog.core.exceptions import (
    ConfigurationError,
    DatasetNotFoundError,
    EmptyGlobMatchError,
)
from datacachalog.core.glob_utils import (
    derive_cache_key,
    is_glob_pattern,
    split_glob_pattern,
)
from datacachalog.core.models import (
    CacheMetadata,
    Dataset,
    ObjectVersion,
    find_version_at,
)
from datacachalog.core.ports import (
    CachePort,
    ExecutorPort,
    NullProgressReporter,
    ProgressReporter,
    StoragePort,
)


class Catalog:
    """Orchestrates dataset fetching with caching and staleness detection."""

    def __init__(
        self,
        datasets: list[Dataset],
        storage: StoragePort,
        cache: CachePort,
        cache_dir: Path | None = None,
        executor: ExecutorPort | None = None,
    ) -> None:
        self._datasets = {d.name: d for d in datasets}
        self._storage = storage
        self._cache = cache
        self._cache_dir = cache_dir
        self._executor = executor

    @classmethod
    def from_directory(
        cls,
        datasets: list[Dataset],
        directory: Path | None = None,
        cache_dir: Path | str = "data",
    ) -> "Catalog":
        """Create Catalog with auto-discovered project root and default adapters.

        Args:
            datasets: List of datasets to register.
            directory: Start directory for root discovery (defaults to cwd).
            cache_dir: Cache directory relative to project root or absolute path.

        Returns:
            Catalog with RouterStorage, FileCache, and resolved paths.
        """
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import create_router
        from datacachalog.config import find_project_root

        root = find_project_root(directory)

        resolved_cache_dir = Path(cache_dir)
        if not resolved_cache_dir.is_absolute():
            resolved_cache_dir = root / resolved_cache_dir

        resolved_datasets = [ds.with_resolved_paths(root) for ds in datasets]

        return cls(
            datasets=resolved_datasets,
            storage=create_router(),
            cache=FileCache(resolved_cache_dir),
            cache_dir=resolved_cache_dir,
        )

    @property
    def datasets(self) -> list[Dataset]:
        """List all registered datasets."""
        return list(self._datasets.values())

    def get_dataset(self, name: str) -> Dataset:
        """Look up a dataset by name.

        Args:
            name: The dataset name.

        Returns:
            The Dataset with the matching name.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
        """
        try:
            return self._datasets[name]
        except KeyError:
            raise DatasetNotFoundError(
                name, available=list(self._datasets.keys())
            ) from None

    def _resolve_cache_path(self, dataset: Dataset) -> Path:
        """Resolve the local cache path for a dataset.

        Uses explicit cache_path if set, otherwise derives from source.
        """
        if dataset.cache_path is not None:
            return dataset.cache_path

        if self._cache_dir is None:
            raise ConfigurationError(
                f"Dataset '{dataset.name}' has no cache_path and no cache_dir configured"
            )

        # Derive from source: extract filename
        # For s3://bucket/path/to/file.ext -> file.ext
        # For /local/path/to/file.ext -> file.ext
        source = dataset.source
        if "://" in source:
            # URI format: extract path after scheme://host/
            path_part = source.split("://", 1)[1]
            # Remove bucket/host part
            filename = path_part.split("/", 1)[1] if "/" in path_part else path_part
        else:
            # Local path
            filename = Path(source).name

        return self._cache_dir / filename

    def _resolve_version_cache_key(
        self, dataset: Dataset, last_modified: datetime
    ) -> str:
        """Generate a date-based cache key for a versioned file.

        Format: YYYY-MM-DDTHHMMSS.ext where ext comes from the original filename.

        Args:
            dataset: The dataset being fetched.
            last_modified: The version's last_modified timestamp.

        Returns:
            Cache key in format YYYY-MM-DDTHHMMSS.ext
        """
        # Extract extension from source filename
        source = dataset.source
        if "://" in source:
            path_part = source.split("://", 1)[1]
            filename = path_part.split("/", 1)[1] if "/" in path_part else path_part
        else:
            filename = Path(source).name

        # Get extension (including the dot)
        ext = Path(filename).suffix

        # Format datetime as YYYY-MM-DDTHHMMSS (no colons, no timezone)
        # Ensure UTC timezone for consistent formatting
        from datetime import UTC

        if last_modified.tzinfo is None:
            dt = last_modified.replace(tzinfo=UTC)
        else:
            dt = last_modified.astimezone(UTC)

        date_str = dt.strftime("%Y-%m-%dT%H%M%S")

        return f"{date_str}{ext}"

    def fetch(
        self,
        name: str,
        progress: ProgressReporter | None = None,
        *,
        version_id: str | None = None,
        as_of: datetime | None = None,
        dry_run: bool = False,
    ) -> Path | list[Path]:
        """Fetch a dataset, downloading if not cached or stale.

        For glob patterns (source contains *, ?, or [), expands the pattern
        and returns a list of paths. For single files, returns a single Path.

        Args:
            name: The dataset name.
            progress: Optional progress reporter for download feedback.
            version_id: Optional S3 version ID to fetch a specific version.
            as_of: Optional datetime to fetch the version active at that time.
            dry_run: If True, check staleness but skip download and cache updates.

        Returns:
            Path to local cached file (single file) or list[Path] (glob pattern).

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
            EmptyGlobMatchError: If glob pattern matches no files.
            ValueError: If both version_id and as_of are provided.
            ValueError: If version_id or as_of used with glob pattern.
            VersionNotFoundError: If no version exists at as_of time.
        """
        if progress is None:
            progress = NullProgressReporter()

        # Validate mutually exclusive parameters
        if version_id is not None and as_of is not None:
            raise ValueError("version_id and as_of are mutually exclusive")

        dataset = self.get_dataset(name)

        # Check for glob + version incompatibility
        if is_glob_pattern(dataset.source):
            if version_id is not None or as_of is not None:
                raise ValueError(
                    "Versioned fetch (version_id or as_of) is not supported "
                    "for glob pattern datasets"
                )
            return self._fetch_glob(dataset, progress, dry_run=dry_run)

        # Resolve as_of to version_id
        if as_of is not None:
            versions = self._storage.list_versions(dataset.source)
            resolved_version = find_version_at(versions, as_of)
            if resolved_version is None:
                from datacachalog.core.exceptions import VersionNotFoundError

                raise VersionNotFoundError(name, as_of)
            version_id = resolved_version.version_id

        # Version-specific fetch
        if version_id is not None:
            return self._fetch_version(dataset, version_id, progress, dry_run=dry_run)

        # Single file fetch (existing logic)
        return self._fetch_single(name, dataset, progress, dry_run=dry_run)

    def _fetch_single(
        self,
        cache_key: str,
        dataset: Dataset,
        progress: ProgressReporter,
        *,
        dry_run: bool = False,
    ) -> Path:
        """Fetch a single file with caching and staleness detection.

        Args:
            cache_key: Key to use for caching (may differ from dataset.name for globs).
            dataset: Dataset with source to fetch.
            progress: Progress reporter for download feedback.
            dry_run: If True, check staleness but skip download and cache updates.

        Returns:
            Path to the local cached file.
        """
        # Check cache
        cached = self._cache.get(cache_key)
        if cached is not None:
            cached_path, cache_meta = cached
            remote_meta = self._storage.head(dataset.source)
            if not cache_meta.is_stale(remote_meta):
                return cached_path

        # In dry-run mode, check staleness but don't download or update cache
        if dry_run:
            # Still need to check remote metadata for staleness
            remote_meta = self._storage.head(dataset.source)
            # Return cached path if exists, otherwise return expected cache path
            if cached is not None:
                return cached_path
            # No cache exists - return expected cache path
            return self._resolve_cache_path(dataset)

        # Cache miss or stale - download with progress
        dest = self._resolve_cache_path(dataset)
        dest.parent.mkdir(parents=True, exist_ok=True)

        remote_meta = self._storage.head(dataset.source)
        total_size = remote_meta.size or 0

        callback = progress.start_task(cache_key, total_size)
        try:
            self._storage.download(dataset.source, dest, callback)
        finally:
            progress.finish_task(cache_key)

        # Store in cache with metadata
        cache_meta = CacheMetadata(
            etag=remote_meta.etag,
            last_modified=remote_meta.last_modified,
            source=dataset.source,
        )
        self._cache.put(cache_key, dest, cache_meta)

        # Return the cache's path (may differ from dest due to cache internals)
        cached_result = self._cache.get(cache_key)
        if cached_result is None:
            # Shouldn't happen - we just put it
            return dest
        return cached_result[0]

    def _fetch_version(
        self,
        dataset: Dataset,
        version_id: str,
        progress: ProgressReporter,
        *,
        dry_run: bool = False,
    ) -> Path:
        """Fetch a specific version of a dataset.

        Downloads the specified version using download_version() and caches
        it under a date-based filename (YYYY-MM-DDTHHMMSS.ext).

        Args:
            dataset: Dataset with source to fetch.
            version_id: S3 version ID to download.
            progress: Progress reporter for download feedback.
            dry_run: If True, check version exists but skip download and cache updates.

        Returns:
            Path to the local cached file.
        """
        # Get version metadata to generate date-based cache key
        remote_meta = self._storage.head_version(dataset.source, version_id)

        # Ensure we have last_modified (required for date-based key)
        if remote_meta.last_modified is None:
            raise ValueError(
                f"Version {version_id} has no last_modified timestamp - cannot generate date-based cache key"
            )

        # Generate date-based cache key (filename)
        cache_key = self._resolve_version_cache_key(dataset, remote_meta.last_modified)

        # Check cache for this specific version
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached[0]

        # In dry-run mode, return expected cache path without downloading
        if dry_run:
            if self._cache_dir is None:
                raise ConfigurationError("cache_dir is required for versioned fetches")
            return self._cache_dir / cache_key

        # Download to a temporary location first, then cache will copy to final location
        import tempfile

        # Ensure cache directory exists
        if self._cache_dir is None:
            raise ConfigurationError("cache_dir is required for versioned fetches")
        self._cache_dir.mkdir(parents=True, exist_ok=True)

        with tempfile.NamedTemporaryFile(delete=False, dir=self._cache_dir) as tmp_file:
            tmp_path = Path(tmp_file.name)

        try:
            total_size = remote_meta.size or 0

            callback = progress.start_task(cache_key, total_size)
            try:
                self._storage.download_version(
                    dataset.source, tmp_path, version_id, callback
                )
            finally:
                progress.finish_task(cache_key)

            # Store in cache with metadata (cache will copy to final date-based location)
            cache_meta = CacheMetadata(
                etag=remote_meta.etag,
                last_modified=remote_meta.last_modified,
                source=dataset.source,
            )
            self._cache.put(cache_key, tmp_path, cache_meta)
        finally:
            # Clean up temporary file if it still exists
            tmp_path.unlink(missing_ok=True)

        # Return the cache's path
        cached_result = self._cache.get(cache_key)
        if cached_result is None:
            # Shouldn't happen - we just put it, but return the cache's expected path
            return self._cache_dir / cache_key
        return cached_result[0]

    def _fetch_glob(
        self,
        dataset: Dataset,
        progress: ProgressReporter,
        *,
        dry_run: bool = False,
    ) -> list[Path]:
        """Fetch all files matching a glob pattern.

        Args:
            dataset: Dataset with glob pattern in source.
            progress: Progress reporter for download feedback.
            dry_run: If True, check staleness but skip downloads and cache updates.

        Returns:
            List of paths to local cached files.

        Raises:
            EmptyGlobMatchError: If no files match the pattern.
        """
        # Split pattern and expand
        prefix, pattern = split_glob_pattern(dataset.source)
        matched_uris = self._storage.list(prefix, pattern)

        if not matched_uris:
            raise EmptyGlobMatchError(pattern, prefix)

        # Fetch each matched file
        paths: list[Path] = []
        for uri in matched_uris:
            # Derive cache key for this specific file
            cache_key = derive_cache_key(dataset.name, prefix, uri)

            # Create a virtual dataset for this single file
            single_dataset = Dataset(
                name=cache_key,
                source=uri,
                description=dataset.description,
            )

            path = self._fetch_single(
                cache_key, single_dataset, progress, dry_run=dry_run
            )
            paths.append(path)

        return paths

    def is_stale(self, name: str) -> bool:
        """Check if a dataset's cache is stale without downloading.

        Args:
            name: The dataset name.

        Returns:
            True if not cached or if remote has changed since caching.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
        """
        dataset = self.get_dataset(name)
        cached = self._cache.get(name)
        if cached is None:
            return True
        _, cache_meta = cached
        remote_meta = self._storage.head(dataset.source)
        return cache_meta.is_stale(remote_meta)

    def invalidate(self, name: str) -> None:
        """Remove a dataset from cache, forcing re-download on next fetch.

        Args:
            name: The dataset name.
        """
        self._cache.invalidate(name)

    def invalidate_glob(self, name: str) -> int:
        """Remove all cached files for a glob pattern dataset.

        For glob datasets, each matched file is cached separately under
        a hierarchical key like "{name}/filename.ext". This method removes
        all such entries.

        Args:
            name: The dataset name.

        Returns:
            Number of cache entries removed.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
            ValueError: If the dataset is not a glob pattern.
        """
        dataset = self.get_dataset(name)
        if not is_glob_pattern(dataset.source):
            raise ValueError(
                f"Dataset '{name}' is not a glob pattern. "
                "Use invalidate() for single-file datasets."
            )
        return self._cache.invalidate_prefix(name)

    def cache_size(self, name: str) -> int:
        """Calculate cache size for a dataset in bytes.

        Includes both the data file and metadata file for the dataset.

        Args:
            name: The dataset name.

        Returns:
            Cache size in bytes. Returns 0 if dataset is not cached.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
        """
        self.get_dataset(name)  # Validate dataset exists
        cached = self._cache.get(name)
        if cached is None:
            return 0

        cached_path, _ = cached
        total_size = 0

        # Add data file size
        if cached_path.exists():
            total_size += cached_path.stat().st_size

        # Add metadata file size
        # Metadata file is typically alongside the data file with .meta.json suffix
        meta_path = cached_path.parent / f"{cached_path.name}.meta.json"
        if not meta_path.exists() and hasattr(self._cache, "cache_dir"):
            # Try alternative location (for flat cache structure)
            meta_path = self._cache.cache_dir / f"{name}.meta.json"

        if meta_path.exists():
            total_size += meta_path.stat().st_size

        return total_size

    def versions(self, name: str, limit: int | None = None) -> list[ObjectVersion]:
        """List available versions of a dataset.

        Returns version history for versioned storage backends (e.g., S3 with
        versioning enabled). Versions are sorted newest-first.

        Args:
            name: The dataset name.
            limit: Maximum number of versions to return.

        Returns:
            List of ObjectVersion with metadata for each version.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
            VersioningNotSupportedError: If storage backend doesn't support versioning.
        """
        dataset = self.get_dataset(name)
        return self._storage.list_versions(dataset.source, limit=limit)

    def push(
        self,
        name: str,
        local_path: Path,
        progress: ProgressReporter | None = None,
    ) -> None:
        """Upload a local file to a dataset's remote source.

        After uploading, updates the cache with the new file and metadata
        so subsequent fetch() calls return the pushed file without re-download.

        Args:
            name: The dataset name.
            local_path: Path to local file to upload.
            progress: Optional progress reporter for upload feedback.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
            FileNotFoundError: If local_path does not exist.
        """
        if progress is None:
            progress = NullProgressReporter()

        dataset = self.get_dataset(name)
        total_size = local_path.stat().st_size

        callback = progress.start_task(name, total_size)
        try:
            self._storage.upload(local_path, dataset.source, callback)
        finally:
            progress.finish_task(name)

        # Update cache with new remote metadata
        remote_meta = self._storage.head(dataset.source)
        cache_meta = CacheMetadata(
            etag=remote_meta.etag,
            last_modified=remote_meta.last_modified,
            source=dataset.source,
        )
        self._cache.put(name, local_path, cache_meta)

    def fetch_all(
        self,
        progress: ProgressReporter | None = None,
        max_workers: int | None = None,
        *,
        dry_run: bool = False,
    ) -> dict[str, Path | list[Path]]:
        """Fetch all datasets, downloading any that are stale.

        Downloads are performed in parallel when an executor is injected and
        max_workers > 1. When no executor is provided, execution is sequential.

        Args:
            progress: Optional progress reporter for download feedback.
            max_workers: Maximum parallel downloads when executor is provided.
                Use 1 for sequential downloads. When executor is None, execution
                is always sequential regardless of max_workers.
            dry_run: If True, check staleness but skip downloads and cache updates.

        Returns:
            Dict mapping dataset names to their local cached paths.
            Glob datasets return list[Path], single-file datasets return Path.
        """
        if progress is None:
            progress = NullProgressReporter()

        datasets = list(self._datasets.values())
        if not datasets:
            return {}

        results: dict[str, Path | list[Path]] = {}

        # Sequential execution for max_workers=1 or when no executor provided
        if max_workers == 1 or self._executor is None:
            for dataset in datasets:
                results[dataset.name] = self.fetch(
                    dataset.name, progress=progress, dry_run=dry_run
                )
            return results

        # Parallel execution - use injected executor
        def fetch_one(dataset: Dataset) -> tuple[str, Path | list[Path]]:
            result = self.fetch(dataset.name, progress=progress, dry_run=dry_run)
            return dataset.name, result

        executor = self._executor
        with executor:
            futures = [executor.submit(fetch_one, ds) for ds in datasets]
            for future in futures:
                result_tuple = future.result()
                # Type narrowing: fetch_one returns tuple[str, Path | list[Path]]
                assert isinstance(result_tuple, tuple)
                name, result = result_tuple
                results[name] = result

        return results
