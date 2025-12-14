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
from datacachalog.config import find_project_root
from datacachalog.core.exceptions import (
    CacheCorruptError,
    CacheError,
    CatalogLoadError,
    ConfigurationError,
    DatacachalogError,
    DatasetNotFoundError,
    StorageAccessError,
    StorageError,
    StorageNotFoundError,
)
from datacachalog.core.models import CacheMetadata, Dataset, FileMetadata
from datacachalog.core.ports import (
    CachePort,
    NullProgressReporter,
    ProgressCallback,
    ProgressReporter,
    StoragePort,
)
from datacachalog.core.services import Catalog
from datacachalog.discovery import discover_catalogs, load_catalog
from datacachalog.progress import RichProgressReporter


__version__ = "0.2.0"

__all__ = [
    "CacheCorruptError",
    "CacheError",
    "CacheMetadata",
    "CachePort",
    "Catalog",
    "CatalogLoadError",
    "ConfigurationError",
    "DatacachalogError",
    "Dataset",
    "DatasetNotFoundError",
    "FileCache",
    "FileMetadata",
    "FilesystemStorage",
    "NullProgressReporter",
    "ProgressCallback",
    "ProgressReporter",
    "RichProgressReporter",
    "RouterStorage",
    "S3Storage",
    "StorageAccessError",
    "StorageError",
    "StorageNotFoundError",
    "StoragePort",
    "__version__",
    "create_router",
    "discover_catalogs",
    "find_project_root",
    "load_catalog",
]
