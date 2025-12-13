"""Basic single-dataset fetch example.

This example shows the simplest usage pattern: define a dataset,
create a catalog, and fetch the data. The library handles caching
and staleness detection automatically.
"""

from pathlib import Path

from datacachalog import Catalog, Dataset, FileCache, FilesystemStorage


# Define a dataset pointing to remote storage
customers = Dataset(
    name="customers",
    source="s3://my-bucket/customers/data.parquet",
    description="Customer master data",
)

# Create catalog with storage and cache adapters
# For S3, use: storage=create_router() instead of FilesystemStorage()
catalog = Catalog(
    datasets=[customers],
    storage=FilesystemStorage(),
    cache=FileCache(Path("./data")),
    cache_dir=Path("./data"),
)

# Fetch downloads if not cached or stale, returns local path
path = catalog.fetch("customers")
print(f"Data available at: {path}")

# Subsequent fetches check staleness via ETag/LastModified
# If remote hasn't changed, returns cached path immediately
path = catalog.fetch("customers")
