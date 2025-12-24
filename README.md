# datacachalog

A data catalog with file-based caching for remote storage.

## Features

- Simple Python API for fetching remote files with caching
- Transparent staleness detection via ETags/LastModified
- S3 and local filesystem storage backends
- Parallel downloads with rich progress bars
- Pure Python core with hexagonal architecture

## Installation

```bash
pip install datacachalog
```

## Quick Start

```python
from datacachalog import Dataset, Catalog

# Define datasets
customers = Dataset(
    name="customers",
    source="s3://bucket/customers/data.parquet",
    description="Customer master data",
)

# Create catalog
catalog = Catalog(
    datasets=[customers],
    cache_dir="./data",
)

# Fetch dataset (downloads if stale, returns cached path otherwise)
path = catalog.fetch("customers")
```

## CLI Usage

The `catalog` CLI provides commands for managing your data catalog:

```bash
# List all datasets
catalog list

# List datasets with cache status (fresh/stale/missing)
catalog list --status

# List datasets from a specific catalog
catalog list --catalog core --status

# Fetch a dataset
catalog fetch customers

# Show cache status for all datasets
catalog status
```

The `--status` flag shows the cache state for each dataset:
- `[fresh]` - Dataset is cached and up-to-date
- `[stale]` - Dataset is cached but remote source has changed
- `[missing]` - Dataset is not cached

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest

# Run type checking
uv run mypy src/datacachalog/

# Run linting
uv run ruff check src/datacachalog/
uv run ruff format src/datacachalog/

# Install pre-commit hooks
uv run pre-commit install
```

## License

MIT
