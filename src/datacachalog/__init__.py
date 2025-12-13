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

from datacachalog.adapters.cache import FileCache
from datacachalog.adapters.storage import (
    FilesystemStorage,
    RouterStorage,
    S3Storage,
    create_router,
)
from datacachalog.core.models import CacheMetadata, Dataset, FileMetadata
from datacachalog.core.ports import CachePort, ProgressCallback, StoragePort
from datacachalog.core.services import Catalog


__version__ = "0.1.1"

__all__ = [
    "CacheMetadata",
    "CachePort",
    "Catalog",
    "Dataset",
    "FileCache",
    "FileMetadata",
    "FilesystemStorage",
    "ProgressCallback",
    "RouterStorage",
    "S3Storage",
    "StoragePort",
    "__version__",
    "create_router",
]
