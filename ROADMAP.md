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
- [ ] `Catalog.from_directory()` factory - auto-discover root and create with sensible defaults

## Future

- Graceful network failure handling (warn and use stale cache if available, raise only if no cache)
- Glob pattern support for multi-file datasets
- GCS and Azure Blob storage adapters
- Optional CLI (`catalog fetch customers`)
- Retry logic for transient failures
- Cache eviction policies (LRU, max size)
- Async API variant

---

## Open Design Questions

Decisions needed before or during implementation:

### Write Path Cache Semantics
After `catalog.push("name", local_path=...)`:
- [ ] `local_path` becomes the cache entry (move/link)
- [ ] `local_path` is copied to cache location

### Post-Push Authority
After push, is remote authoritative?
- [ ] Yes - next fetch pulls from S3, ignores what you just pushed
- [ ] No - local file is trusted until remote changes

### Credentials
S3 authentication strategy:
- [ ] Environment variables only (AWS_*)
- [ ] Explicit credentials in code
- [ ] boto3's full credential chain (env, ~/.aws, IAM role, etc.)
