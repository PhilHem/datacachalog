# Roadmap

## Phase 1: Core Foundation

- [x] Project setup (pyproject.toml, uv, ruff, mypy, pytest)
- [x] Pre-commit hooks (ruff check, ruff format, mypy, pytest)
- [x] Core models: `Dataset`, `CacheMetadata`, `FileMetadata`
- [x] Port definitions: `StoragePort`, `CachePort`
- [x] Filesystem storage adapter (for local dev/testing)
- [x] Export `FilesystemStorage` from package `__init__.py`
- [x] File cache adapter with JSON metadata sidecar
- [x] Basic `Catalog` class with `fetch()` returning `Path`
- [x] `catalog.datasets` property to list registered datasets
- [x] Cache path derivation from source (path without bucket, preserves hierarchy)

## Phase 2: S3 Integration

- [x] S3 storage adapter using boto3
- [x] `head()` operation for metadata without download
- [x] ETag/LastModified staleness detection
- [x] `is_stale()` check without download
- [x] `invalidate()` to force re-download
- [x] Export S3Storage from package root `__init__.py`
- [x] Integration tests with moto or localstack
- [x] S3 URI scheme validation in Catalog (route `s3://` to S3Storage via `RouterStorage`)

## Phase 3: Progress & Parallelism

- [x] Rich progress bar integration
- [x] Per-file download progress (bytes)
- [x] `fetch_all()` with parallel downloads
- [x] Aggregate progress display for concurrent fetches

## Phase 4: Write Support

- [x] `catalog.push(name, local_path)` implementation
- [x] Upload to S3 with progress
- [x] Cache update after push

## Phase 5: Polish

- [x] Error handling and meaningful exceptions
- [x] Documentation and examples

## Phase 6: Configuration Ergonomics

- [x] `Dataset.with_resolved_paths(root)` - resolve relative cache_path against project root
- [x] `Catalog.from_directory()` factory - auto-discover root and create with sensible defaults

## Phase 7: Design Decisions

Resolved design questions that inform future implementation:

- [x] **Write Path Cache Semantics**: Copy to cache location (user retains original, cache owns its files for safe eviction)
- [x] **Post-Push Authority**: Remote is authoritative (cache stores remote ETag after push, so next fetch skips download if unchanged)
- [x] **Credentials**: boto3's full credential chain (adapter delegates to boto3 defaults, with optional client injection for custom auth)

## Phase 8: CLI

- [x] Add `typer` dependency
- [x] CLI entry point via `catalog` command
- [x] `catalog init` - scaffold project structure (`.datacachalog/catalogs/`, `data/` dirs)
- [x] `catalog list` - show registered datasets (with `--catalog` filter)
- [x] Catalog discovery (`discover_catalogs`, `load_catalog` from `.datacachalog/catalogs/*.py`)
- [x] `catalog fetch <name>` - fetch single dataset
- [x] `catalog fetch --all` - fetch all datasets
- [x] `catalog status` - show cache state (cached/stale/missing) per dataset
- [x] `catalog invalidate <name>` - force re-download on next fetch
- [x] Error handling in catalog discovery (graceful syntax error messages)

## Phase 9: Glob Patterns

- [x] Glob pattern support in `source` field (`s3://bucket/data/*.parquet`)
- [x] `fetch()` returns `list[Path]` for glob datasets
- [x] Parallel download of matched files
- [x] Staleness check per-file (only download changed files)
- [x] Cache path derivation for glob matches (preserve relative structure)
- [x] `catalog.invalidate_glob(name)` - clear all cached files for a glob pattern
- [ ] CLI `invalidate-glob <name>` command
- [ ] S3 glob integration test with moto

## Phase 10: S3 Version Tracking

Access historical versions of objects in versioned S3 buckets, with date-based selection.

### Core

- [ ] `ObjectVersion` model (last_modified, version_id, etag, size, is_latest, is_delete_marker)
- [ ] Extend `StoragePort` with `list_versions()`, `download_version()`, `head_version()`
- [ ] Version resolution: `as_of=datetime` finds closest version at or before that time
- [ ] Version-aware cache key strategy (date-based paths: `2024-12-14T093000.parquet`)

### S3 Adapter

- [ ] `list_versions()` via `list_object_versions()`, sorted by `last_modified` descending
- [ ] `download_version()` with `VersionId` parameter
- [ ] Handle delete markers gracefully (skip or raise clear error)

### Filesystem Adapter

- [ ] Raise `VersioningNotSupportedError` for version methods

### Library API

- [ ] `catalog.versions(name, limit=10) -> list[ObjectVersion]`
- [ ] `catalog.fetch(name, as_of=datetime)` - fetch version at point in time
- [ ] `catalog.fetch(name, version_id=str)` - fetch specific version (escape hatch)

### CLI

- [ ] `catalog versions <dataset>` - list available versions with dates, sizes
- [ ] `catalog versions <dataset> --limit=N` - control number shown
- [ ] `catalog fetch <dataset> --as-of=2024-12-10` - fetch version from date
- [ ] Human-friendly output: date as primary identifier, version_id hidden

## Future

- `catalog list --status` flag - combine list and status output
- Graceful network failure handling (warn and use stale cache if available, raise only if no cache)
- GCS and Azure Blob storage adapters
- Retry logic for transient failures
- Cache eviction policies (LRU, max size)
- Async API variant
