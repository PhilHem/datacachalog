"""Core domain services for datacachalog."""

from concurrent.futures import ThreadPoolExecutor
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
from datacachalog.core.models import CacheMetadata, Dataset
from datacachalog.core.ports import (
    CachePort,
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
    ) -> None:
        self._datasets = {d.name: d for d in datasets}
        self._storage = storage
        self._cache = cache
        self._cache_dir = cache_dir

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

    def fetch(
        self,
        name: str,
        progress: ProgressReporter | None = None,
    ) -> Path | list[Path]:
        """Fetch a dataset, downloading if not cached or stale.

        For glob patterns (source contains *, ?, or [), expands the pattern
        and returns a list of paths. For single files, returns a single Path.

        Args:
            name: The dataset name.
            progress: Optional progress reporter for download feedback.

        Returns:
            Path to local cached file (single file) or list[Path] (glob pattern).

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
            EmptyGlobMatchError: If glob pattern matches no files.
        """
        if progress is None:
            progress = NullProgressReporter()

        dataset = self.get_dataset(name)

        # Check if this is a glob pattern
        if is_glob_pattern(dataset.source):
            return self._fetch_glob(dataset, progress)

        # Single file fetch (existing logic)
        return self._fetch_single(name, dataset, progress)

    def _fetch_single(
        self,
        cache_key: str,
        dataset: Dataset,
        progress: ProgressReporter,
    ) -> Path:
        """Fetch a single file with caching and staleness detection.

        Args:
            cache_key: Key to use for caching (may differ from dataset.name for globs).
            dataset: Dataset with source to fetch.
            progress: Progress reporter for download feedback.

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

    def _fetch_glob(
        self,
        dataset: Dataset,
        progress: ProgressReporter,
    ) -> list[Path]:
        """Fetch all files matching a glob pattern.

        Args:
            dataset: Dataset with glob pattern in source.
            progress: Progress reporter for download feedback.

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

            path = self._fetch_single(cache_key, single_dataset, progress)
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
    ) -> dict[str, Path | list[Path]]:
        """Fetch all datasets, downloading any that are stale.

        Downloads are performed in parallel when max_workers > 1.

        Args:
            progress: Optional progress reporter for download feedback.
            max_workers: Maximum parallel downloads. None uses ThreadPoolExecutor
                default. Use 1 for sequential downloads.

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

        # Sequential execution for max_workers=1
        if max_workers == 1:
            for dataset in datasets:
                results[dataset.name] = self.fetch(dataset.name, progress=progress)
            return results

        # Parallel execution
        def fetch_one(dataset: Dataset) -> tuple[str, Path | list[Path]]:
            result = self.fetch(dataset.name, progress=progress)
            return dataset.name, result

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_one, ds) for ds in datasets]
            for future in futures:
                name, result = future.result()
                results[name] = result

        return results
