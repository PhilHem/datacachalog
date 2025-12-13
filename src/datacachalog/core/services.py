"""Core domain services for datacachalog."""

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from datacachalog.core.exceptions import ConfigurationError, DatasetNotFoundError
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
    ) -> Path:
        """Fetch a dataset, downloading if not cached or stale.

        Args:
            name: The dataset name.
            progress: Optional progress reporter for download feedback.

        Returns:
            Path to the local cached file.

        Raises:
            DatasetNotFoundError: If no dataset with that name exists.
        """
        if progress is None:
            progress = NullProgressReporter()

        dataset = self.get_dataset(name)

        # Check cache
        cached = self._cache.get(name)
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

        callback = progress.start_task(name, total_size)
        try:
            self._storage.download(dataset.source, dest, callback)
        finally:
            progress.finish_task(name)

        # Store in cache with metadata
        cache_meta = CacheMetadata(
            etag=remote_meta.etag,
            last_modified=remote_meta.last_modified,
            source=dataset.source,
        )
        self._cache.put(name, dest, cache_meta)

        # Return the cache's path (may differ from dest due to cache internals)
        cached_result = self._cache.get(name)
        if cached_result is None:
            # Shouldn't happen - we just put it
            return dest
        return cached_result[0]

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
    ) -> dict[str, Path]:
        """Fetch all datasets, downloading any that are stale.

        Downloads are performed in parallel when max_workers > 1.

        Args:
            progress: Optional progress reporter for download feedback.
            max_workers: Maximum parallel downloads. None uses ThreadPoolExecutor
                default. Use 1 for sequential downloads.

        Returns:
            Dict mapping dataset names to their local cached paths.
        """
        if progress is None:
            progress = NullProgressReporter()

        datasets = list(self._datasets.values())
        if not datasets:
            return {}

        results: dict[str, Path] = {}

        # Sequential execution for max_workers=1
        if max_workers == 1:
            for dataset in datasets:
                results[dataset.name] = self.fetch(dataset.name, progress=progress)
            return results

        # Parallel execution
        def fetch_one(dataset: Dataset) -> tuple[str, Path]:
            path = self.fetch(dataset.name, progress=progress)
            return dataset.name, path

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(fetch_one, ds) for ds in datasets]
            for future in futures:
                name, path = future.result()
                results[name] = path

        return results
