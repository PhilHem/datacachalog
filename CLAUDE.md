# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

A Python library providing a **data catalog** with **file-based caching** for remote storage:
- Hexagonal architecture (pure core, ports, adapters)
- S3 and local filesystem storage backends
- File-based caching with staleness detection via ETags/LastModified
- Parallel downloads with rich progress bars
- Python-first configuration using dataclasses
- Library-first design with optional CLI

## Build Commands

```bash
# Install dependencies
uv sync

# Run all tests
uv run pytest

# Run single test
uv run pytest tests/unit/test_models.py::test_dataset -v

# Type checking
uv run mypy src/datacachalog/

# Linting
uv run ruff check src/datacachalog/
uv run ruff format src/datacachalog/
```

### Package Management (uv only)

**Never edit `pyproject.toml` dependencies directly.** Always use uv commands:

```bash
uv add requests              # Add dependency
uv add --dev pytest          # Add dev dependency
uv remove requests           # Remove dependency
uv lock                      # Update lock file
```

Ruff configuration lives in `pyproject.toml` under `[tool.ruff]`.

### Version Bumping

When bumping the version in `pyproject.toml`, always run `uv lock` afterward to update the lock file:

```bash
# After editing version in pyproject.toml
uv lock
```

### Release Tracking Files

This project uses `CHANGELOG.md` and `ROADMAP.md` to track changes:
- **CHANGELOG.md** - Version history with changes per release
- **ROADMAP.md** - Development phases and progress

Before releasing, check if these files need updates based on commits since last tag.

## Architecture

```
              +-----------------------+
              |      User Code        |
              |  catalog.fetch(name)  |
              +-----------------------+
                         |
              +-----------------------+
              |      Core Domain      |
              |  Catalog, Dataset     |
              +-----------------------+
                    |         |
                    v         v
          +------------+  +------------+
          |   Cache    |  |  Storage   |
          |  (files)   |  |  Adapters  |
          +------------+  +------------+
                          (S3, Local FS)
```

### Core (`datacachalog/core/`)
- **Pure Python only** - no I/O, no network, no file access
- Contains: data models, ports (Protocol interfaces), domain services
- Models: `Dataset` (name, source, description, cache_path), `CacheMetadata` (etag, last_modified)

### Ports (`datacachalog/core/ports.py`)
```python
class StoragePort(Protocol):
    """Remote storage backend (S3, local filesystem)"""
    def download(self, source: str, dest: Path, progress: ProgressCallback) -> None: ...
    def upload(self, local: Path, dest: str) -> None: ...
    def head(self, source: str) -> FileMetadata: ...  # ETag, LastModified

class CachePort(Protocol):
    """Local file cache with metadata tracking"""
    def get(self, key: str) -> tuple[Path, CacheMetadata] | None: ...
    def put(self, key: str, path: Path, metadata: CacheMetadata) -> None: ...
    def invalidate(self, key: str) -> None: ...
```

### Adapters (`datacachalog/adapters/`)
- **Storage**: `s3.py`, `filesystem.py`
- **Cache**: `file_cache.py` (stores files + metadata JSON sidecar)

### Exception Handling

Domain exceptions live in `core/exceptions.py`. All inherit from `DatacachalogError`:

- **Adapters translate errors**: S3 `ClientError` → `StorageNotFoundError`, etc.
- **Core never catches adapter exceptions**: The port boundary is where translation happens
- **`recovery_hint` property**: Each exception provides actionable guidance
- **`ValueError` for programmer errors**: Invalid URIs, unknown schemes stay as `ValueError`

```python
try:
    path = catalog.fetch("customers")
except DatasetNotFoundError as e:
    print(e.recovery_hint)  # "Available datasets: orders, products"
except StorageNotFoundError as e:
    print(e.recovery_hint)  # "Verify the source path exists: s3://..."
except DatacachalogError:
    # Catch-all for any library error
    pass
```

Exception hierarchy:
- `DatacachalogError` - base for all library errors
  - `DatasetNotFoundError` - dataset name not in catalog
  - `StorageError` - base for storage errors
    - `StorageNotFoundError` - file/object doesn't exist
    - `StorageAccessError` - permission denied
  - `CacheError` - base for cache errors
    - `CacheCorruptError` - metadata JSON unreadable
  - `ConfigurationError` - missing required config

## Folder Structure

```
src/datacachalog/
├── __init__.py          # Re-exports: Dataset, Catalog
├── core/
│   ├── exceptions.py    # Domain exceptions with recovery hints
│   ├── models.py        # Dataset, CacheMetadata, FileMetadata
│   ├── services.py      # Catalog orchestration logic
│   └── ports.py         # StoragePort, CachePort protocols
├── adapters/
│   ├── storage/
│   │   ├── s3.py
│   │   └── filesystem.py
│   └── cache/
│       └── file_cache.py
├── progress/
│   └── rich_progress.py # Rich-based progress bars
└── cli/
    └── __main__.py      # Optional CLI entry point
examples/
└── basic_usage.py
tests/
├── unit/
├── integration/
└── e2e/
```

## API Design

### Configuration (Python dataclasses)

```python
from datacachalog import Dataset, Catalog

customers = Dataset(
    name="customers",
    source="s3://bucket/customers/data.parquet",
    description="Customer master data",
)

transactions = Dataset(
    name="transactions",
    source="s3://bucket/transactions/2024.parquet",
    cache_path="./data/transactions.parquet",  # explicit local path
)

catalog = Catalog(
    datasets=[customers, transactions],
    cache_dir="./data",  # default for datasets without explicit cache_path
)
```

### Core Operations

```python
# Download if stale, return local path
path: Path = catalog.fetch("customers")

# Parallel fetch all datasets with progress bars
paths: dict[str, Path] = catalog.fetch_all()

# Upload local file to S3
catalog.push("customers", local_path="./output/customers.parquet")

# Check staleness without downloading
is_stale: bool = catalog.is_stale("customers")

# Force next fetch to re-download
catalog.invalidate("customers")
```

### Cache Path Resolution

- Relative paths resolve from **project root**
- If no `cache_path` specified, derived from source:
  - `s3://bucket/path/to/file.parquet` → `{cache_dir}/path/to/file.parquet`
- Cache stored in explicit `cache_dir`, not global location

### Staleness Detection

- Uses S3 ETag and LastModified headers
- `catalog.fetch()` always checks staleness before returning cached file
- If remote unchanged, returns cached path immediately (no download)

### Progress Display

- Per-file progress bars showing bytes downloaded
- Aggregate progress for concurrent parallel downloads
- Uses `rich` library for terminal output

## Key Constraints

1. Core must never import I/O or network code
2. Adapters implement Port protocols
3. Staleness always checked - no silent stale data
4. File formats are opaque - library doesn't parse file contents
5. One file = one dataset (no glob patterns initially)

## Balancing Abstraction vs Coupling

Hexagonal architecture can lead to over-engineering. Use these indicators:

### Signs of unnecessary indirection
- A port with only one adapter that will never have another
- Wrapper classes that just delegate to another class
- Interfaces with a single method that could be a function
- "Manager", "Handler", "Processor" classes that don't manage state
- More than 3 layers between API call and storage

### Signs of problematic coupling
- Core importing from `adapters/`
- Storage adapter importing progress bar library
- Tests requiring S3 or network to run
- Changing a model requires updating more than 3 files

### Rules of thumb
- **No port without 2+ adapters** (or clear intent for future adapters)
- **Prefer functions over classes** for stateless operations
- **Adapters should be thin** - convert types and delegate, not implement logic
- **If unsure, start concrete** - extract abstraction when the second use case appears

## Development Workflow (TDD)

This architecture is designed for test-driven development. Always write tests first:

1. **Define the port interface** in `core/ports.py`
2. **Write failing tests** against the port using filesystem adapter
3. **Implement core logic** until tests pass
4. **Add S3 adapter** that satisfies the same port

The filesystem adapter is not a test double - it's a production-ready implementation for local development. Unit tests use it directly, keeping tests fast and deterministic.

### Planning Review

After completing any significant implementation, review for roadmap opportunities:

1. **Read ROADMAP.md first** to understand current phases, existing items, and future directions - suggestions must NOT duplicate anything already listed

2. **Identify 3 quick wins** that:
   - Build directly on what was just implemented
   - Are small in scope (1-2 TDD cycles)
   - Complete a natural workflow or fill an obvious gap
   - Are NOT already in the roadmap (check all phases including Future)

3. **Present to user** before adding to `ROADMAP.md`

4. **Use `/update-roadmap`** command to trigger this review

This keeps the roadmap fresh with achievable next steps that leverage recent work.

### Test Organization

- **Unit tests** (`tests/unit/`): Core only, use filesystem adapter, no network
- **Integration tests** (`tests/integration/`): Test S3 adapter with localstack or moto
- **E2E tests** (`tests/e2e/`): Full catalog operations against real S3

### Pytest Marks & CI

Tests are marked by architectural component for separate GitHub Actions jobs:

```python
@pytest.mark.core        # Core models, ports, services
@pytest.mark.storage     # Storage adapters (s3, filesystem)
@pytest.mark.cache       # File cache adapter
@pytest.mark.progress    # Rich progress integration
@pytest.mark.cli         # CLI tests
@pytest.mark.e2e         # End-to-end tests
```

Run specific components:
```bash
pytest -m core
pytest -m "storage and not s3"  # filesystem only
pytest -m cli
```

CI runs each mark as a separate job for clear failure isolation.

### CI Parity

**CI must use identical commands to local development.** No separate CI-specific scripts or logic. The GitHub Actions workflow uses `uv sync` and `uv run` exactly as developers do locally. This ensures:
- What passes locally passes in CI
- No "works on my machine" issues
- Single source of truth for how to run tests

### Pre-commit Hooks

Pre-commit hooks mirror the CI pipeline to catch issues before they reach CI. The hooks run:
1. `ruff check` - Linting
2. `ruff format --check` - Format verification
3. `mypy` - Type checking
4. `pytest` - Tests

Install hooks with:
```bash
uv run pre-commit install
```

Run manually:
```bash
uv run pre-commit run --all-files
```

The hooks use the same commands as CI, maintaining parity between local development, pre-commit, and CI.

## Extending the System

### Adding a Storage Backend (e.g., GCS, Azure Blob)
1. Create module in `adapters/storage/`
2. Implement `StoragePort` (download, upload, head)
3. Write integration tests with emulator or mock

### Adding CLI Commands
1. Add command in `cli/`
2. Import core Catalog, call same methods as library
3. Wire up rich progress automatically

## Goals & Non-Goals

### Goals
- Simple Python API for fetching remote files with caching
- Transparent staleness detection via ETags/LastModified
- Parallel downloads with clear progress feedback
- S3 and local filesystem support
- Library-first with optional CLI
- Minimal dependencies (boto3, rich)

### Non-Goals
- Not a data processing library - doesn't read file contents
- Not a data lake or warehouse
- No schema introspection or data validation
- No glob patterns or partitioned datasets (initially)
- No distributed caching
- No framework integrations (FastAPI, Django)

## Future Directions

- Glob pattern support for multi-file datasets
- GCS and Azure Blob storage adapters
- Optional CLI for common operations
- Retry logic for failed downloads
- Cache eviction policies (LRU, size-based)
