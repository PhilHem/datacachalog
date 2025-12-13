"""Error handling patterns with recovery hints.

This example demonstrates how to handle common errors and use
the recovery_hint property to provide actionable guidance.
"""

from pathlib import Path

from datacachalog import (
    Catalog,
    # Exceptions
    DatacachalogError,
    Dataset,
    DatasetNotFoundError,
    FileCache,
    FilesystemStorage,
    StorageAccessError,
    StorageNotFoundError,
)


# Create a catalog with some datasets
catalog = Catalog(
    datasets=[
        Dataset(name="orders", source="s3://bucket/orders.csv"),
        Dataset(name="products", source="s3://bucket/products.csv"),
    ],
    storage=FilesystemStorage(),
    cache=FileCache(Path("./data")),
    cache_dir=Path("./data"),
)


# Pattern 1: Handle unknown dataset names
def fetch_with_suggestions(catalog: Catalog, name: str) -> Path:
    """Fetch dataset with helpful error messages."""
    try:
        return catalog.fetch(name)
    except DatasetNotFoundError as e:
        # recovery_hint lists available datasets
        print(f"Dataset '{name}' not found.")
        print(f"Hint: {e.recovery_hint}")
        raise


# Pattern 2: Handle missing remote files
def fetch_with_storage_fallback(catalog: Catalog, name: str) -> Path | None:
    """Fetch dataset, returning None if remote file doesn't exist."""
    try:
        return catalog.fetch(name)
    except StorageNotFoundError as e:
        print(f"Remote file not found: {e.source}")
        print(f"Hint: {e.recovery_hint}")
        return None


# Pattern 3: Handle permission errors
def fetch_with_access_check(catalog: Catalog, name: str) -> Path | None:
    """Fetch dataset, handling permission errors gracefully."""
    try:
        return catalog.fetch(name)
    except StorageAccessError as e:
        print(f"Access denied to: {e.source}")
        print(f"Hint: {e.recovery_hint}")
        return None


# Pattern 4: Catch-all for any library error
def fetch_safe(catalog: Catalog, name: str) -> Path | None:
    """Fetch dataset with comprehensive error handling."""
    try:
        return catalog.fetch(name)
    except DatasetNotFoundError as e:
        print(f"Unknown dataset: {e.name}")
        print(f"Available: {e.available}")
        return None
    except StorageNotFoundError as e:
        print(f"File not found: {e.source}")
        return None
    except StorageAccessError as e:
        print(f"Permission denied: {e.source}")
        return None
    except DatacachalogError as e:
        # Catch any other library errors
        print(f"Unexpected error: {e}")
        print(f"Hint: {e.recovery_hint}")
        return None


# Example usage
if __name__ == "__main__":
    # This will print error message and re-raise DatasetNotFoundError
    fetch_with_suggestions(catalog, "unknown_dataset")
