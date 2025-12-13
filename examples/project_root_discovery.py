"""Project root discovery for portable relative paths.

This example shows how to use find_project_root() to resolve cache paths
relative to the project root, regardless of where the script is run from.

The function searches upward for marker files in this order:
1. .datacachalog - Explicit project marker
2. pyproject.toml - Python project root
3. .git - Version control root
"""

from datacachalog import (
    Catalog,
    Dataset,
    FileCache,
    FilesystemStorage,
    find_project_root,
)


# Discover project root (works from any subdirectory)
project_root = find_project_root()
print(f"Project root: {project_root}")

# Resolve cache directory relative to project root
cache_dir = project_root / "data"
cache_dir.mkdir(exist_ok=True)

# Define datasets
customers = Dataset(
    name="customers",
    source="s3://my-bucket/customers/data.parquet",
    description="Customer master data",
)

# Create catalog with absolute cache path
catalog = Catalog(
    datasets=[customers],
    storage=FilesystemStorage(),
    cache=FileCache(cache_dir),
    cache_dir=cache_dir,
)

# Fetch will work regardless of current working directory
path = catalog.fetch("customers")
print(f"Data cached at: {path}")
