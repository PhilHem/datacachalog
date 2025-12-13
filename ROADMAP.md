# Roadmap

## Phase 1: Core Foundation

- [x] Project setup (pyproject.toml, uv, ruff, mypy, pytest)
- [x] Pre-commit hooks (ruff check, ruff format, mypy, pytest)
- [x] Core models: `Dataset`, `CacheMetadata`, `FileMetadata`
- [x] Port definitions: `StoragePort`, `CachePort`
- [x] Filesystem storage adapter (for local dev/testing)
- [ ] Export `FilesystemStorage` from package `__init__.py`
- [ ] File cache adapter with JSON metadata sidecar
- [ ] Basic `Catalog` class with `fetch()` returning `Path`

## Phase 2: S3 Integration

- [ ] S3 storage adapter using boto3
- [ ] ETag/LastModified staleness detection
- [ ] Integration tests with moto or localstack
- [ ] `head()` operation for metadata without download

## Phase 3: Progress & Parallelism

- [ ] Rich progress bar integration
- [ ] Per-file download progress (bytes)
- [ ] `fetch_all()` with parallel downloads
- [ ] Aggregate progress display for concurrent fetches

## Phase 4: Write Support

- [ ] `catalog.push(name, local_path)` implementation
- [ ] Upload to S3 with progress
- [ ] Cache update after push

## Phase 5: Polish

- [ ] `is_stale()` check without download
- [ ] `invalidate()` to force re-download
- [ ] Error handling and meaningful exceptions
- [ ] Documentation and examples

## Future

- Glob pattern support for multi-file datasets
- GCS and Azure Blob storage adapters
- Optional CLI (`catalog fetch customers`)
- Retry logic for transient failures
- Cache eviction policies (LRU, max size)
- Async API variant

---

## Open Design Questions

Decisions needed before or during implementation:

### Project Root Discovery
How do we find project root for resolving relative paths?
- [ ] Look for `.git` directory
- [ ] Look for `pyproject.toml`
- [ ] Marker file like `.datacachalog`
- [ ] Explicit configuration only

### Source Path Derivation
For `s3://bucket/path/to/file.parquet` with `cache_dir="./data"`, what's the local path?
- [ ] `./data/file.parquet` (filename only)
- [ ] `./data/bucket/path/to/file.parquet` (full path with bucket)
- [ ] `./data/path/to/file.parquet` (path without bucket)

### Network Failure Behavior
If S3 is unreachable during staleness check:
- [ ] Fail loudly (raise exception)
- [ ] Warn and use stale cache if available

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
