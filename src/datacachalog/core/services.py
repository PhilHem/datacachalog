"""Core domain services for datacachalog."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from datacachalog.core.exceptions import (
    ConfigurationError,
    DatasetNotFoundError,
)
from datacachalog.core.glob_utils import is_glob_pattern
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


if TYPE_CHECKING:
    from datacachalog.core.ports import Reader


class Catalog:
    """Orchestrates dataset fetching with caching and staleness detection."""

    def __init__(
        self,
        datasets: list[Dataset],
        storage: StoragePort,
        cache: CachePort,
        cache_dir: Path | None = None,
        executor: ExecutorPort | None = None,
        reader: Reader[object] | None = None,
    ) -> None:
        self._datasets = {d.name: d for d in datasets}
        self._storage = storage
        self._cache = cache
        self._cache_dir = cache_dir
        self._executor = executor
        self._reader = reader

    @classmethod
    def from_directory(
        cls,
        datasets: list[Dataset],
        directory: Path | None = None,
        cache_dir: Path | str = "data",
    ) -> Catalog:
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
        """Generate a date-based cache key for a versioned file."""
        from datacachalog.core.path_utils import resolve_version_cache_key

        return resolve_version_cache_key(dataset.source, last_modified)

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
            from datacachalog.core.fetch_operations import fetch_glob

            return fetch_glob(
                dataset,
                progress,
                self._storage,
                self._cache,
                self._resolve_cache_path,
                dry_run=dry_run,
            )

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
            from datacachalog.core.fetch_operations import fetch_version

            return fetch_version(
                dataset,
                version_id,
                progress,
                self._storage,
                self._cache,
                self._cache_dir,
                self._resolve_version_cache_key,
                dry_run=dry_run,
            )

        # Single file fetch
        from datacachalog.core.fetch_operations import fetch_single

        return fetch_single(
            name,
            dataset,
            progress,
            self._storage,
            self._cache,
            self._resolve_cache_path,
            dry_run=dry_run,
        )

    def load(
        self,
        name: str,
        progress: ProgressReporter | None = None,
        *,
        version_id: str | None = None,
        as_of: datetime | None = None,
        dry_run: bool = False,
    ) -> object | Path | list[object] | list[Path]:
        """Fetch a dataset and load it using the configured reader.

        This is a convenience method that combines fetch() with reader.read().
        If dry_run=True, returns the Path(s) without calling the reader.

        Args:
            name: The dataset name.
            progress: Optional progress reporter for download feedback.
            version_id: Optional S3 version ID to fetch a specific version.
            as_of: Optional datetime to fetch the version active at that time.
            dry_run: If True, return Path(s) without calling reader.

        Returns:
            The loaded data from reader.read(), or Path(s) if dry_run=True.
            For glob patterns, returns list of loaded objects (or list[Path] if dry_run).

        Raises:
            ReaderNotConfiguredError: If no reader is configured and dry_run=False.
            DatasetNotFoundError: If no dataset with that name exists.
        """
        from datacachalog.core.exceptions import ReaderNotConfiguredError

        # Fetch the data (handles caching, staleness, versioning)
        result = self.fetch(
            name,
            progress=progress,
            version_id=version_id,
            as_of=as_of,
            dry_run=dry_run,
        )

        # In dry-run mode, just return the path(s)
        if dry_run:
            return result

        # Check that reader is configured
        if self._reader is None:
            raise ReaderNotConfiguredError(name)

        # Load single file or list of files
        if isinstance(result, list):
            return [self._reader.read(path) for path in result]
        return self._reader.read(result)

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

        Returns 0 if not cached. Raises DatasetNotFoundError if name unknown.
        """
        from datacachalog.core.cache_maintenance import calculate_cache_size

        return calculate_cache_size(name, self._datasets, self._cache)

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

    def clean_orphaned(self) -> int:
        """Remove orphaned cache entries not belonging to any dataset.

        Compares all cache keys against known dataset cache keys and removes
        orphaned entries. Valid keys include:
        - Regular dataset keys (dataset name)
        - Glob dataset keys (starting with "{dataset_name}/")
        - Versioned keys (date-based format: YYYY-MM-DDTHHMMSS.ext)

        Returns:
            Number of orphaned cache entries removed.
        """
        from datacachalog.core.cache_maintenance import clean_orphaned_keys

        return clean_orphaned_keys(self._cache, self._datasets)
