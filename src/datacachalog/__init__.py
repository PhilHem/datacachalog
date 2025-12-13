"""datacachalog - A data catalog with file-based caching for remote storage.

This library provides a simple Python API for fetching remote files with
transparent caching and staleness detection via ETags/LastModified headers.

Example:
    >>> from datacachalog import Dataset, Catalog
    >>> customers = Dataset(
    ...     name="customers",
    ...     source="s3://bucket/customers/data.parquet",
    ... )
    >>> catalog = Catalog(datasets=[customers], cache_dir="./data")
    >>> path = catalog.fetch("customers")  # Downloads if stale
"""

from datacachalog.core.models import CacheMetadata, Dataset, FileMetadata
from datacachalog.core.ports import CachePort, ProgressCallback, StoragePort


__version__ = "0.1.0"

__all__ = [
    "CacheMetadata",
    "CachePort",
    "Dataset",
    "FileMetadata",
    "ProgressCallback",
    "StoragePort",
    "__version__",
]
