"""Core domain services for datacachalog."""

from pathlib import Path

from datacachalog.core.models import CacheMetadata, Dataset
from datacachalog.core.ports import CachePort, StoragePort


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
            KeyError: If no dataset with that name exists.
        """
        return self._datasets[name]

    def _resolve_cache_path(self, dataset: Dataset) -> Path:
        """Resolve the local cache path for a dataset.

        Uses explicit cache_path if set, otherwise derives from source.
        """
        if dataset.cache_path is not None:
            return dataset.cache_path

        if self._cache_dir is None:
            raise ValueError(
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

    def fetch(self, name: str) -> Path:
        """Fetch a dataset, downloading if not cached or stale.

        Args:
            name: The dataset name.

        Returns:
            Path to the local cached file.

        Raises:
            KeyError: If no dataset with that name exists.
        """
        dataset = self.get_dataset(name)

        # Check cache
        cached = self._cache.get(name)
        if cached is not None:
            cached_path, cache_meta = cached
            remote_meta = self._storage.head(dataset.source)
            if not cache_meta.is_stale(remote_meta):
                return cached_path

        # Cache miss or stale - download
        dest = self._resolve_cache_path(dataset)

        dest.parent.mkdir(parents=True, exist_ok=True)
        remote_meta = self._storage.head(dataset.source)
        self._storage.download(dataset.source, dest, lambda _downloaded, _total: None)

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
