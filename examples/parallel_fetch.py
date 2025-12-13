"""Parallel fetch example with multiple datasets.

This example shows how to fetch multiple datasets efficiently using
fetch_all(), which downloads datasets in parallel with progress bars.
"""

from pathlib import Path

from datacachalog import (
    Catalog,
    Dataset,
    FileCache,
    FilesystemStorage,
    RichProgressReporter,
)


# Define multiple datasets
datasets = [
    Dataset(
        name="orders",
        source="s3://analytics/orders/2024.parquet",
        description="Order transactions",
    ),
    Dataset(
        name="products",
        source="s3://analytics/products/catalog.parquet",
        description="Product catalog",
    ),
    Dataset(
        name="customers",
        source="s3://analytics/customers/master.parquet",
        description="Customer master data",
    ),
]

# Create catalog
catalog = Catalog(
    datasets=datasets,
    storage=FilesystemStorage(),  # Use create_router() for real S3
    cache=FileCache(Path("./data")),
    cache_dir=Path("./data"),
)

# Fetch all datasets in parallel with progress display
with RichProgressReporter() as progress:
    paths = catalog.fetch_all(progress=progress)

# paths is a dict: {"orders": Path(...), "products": Path(...), ...}
for name, path in paths.items():
    print(f"{name}: {path}")

# You can also control parallelism
paths = catalog.fetch_all(max_workers=2)  # Limit to 2 concurrent downloads
