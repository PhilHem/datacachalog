# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.0] - 2025-12-14

### Added

- **Glob Pattern Support** - Multi-file datasets with wildcard patterns
  - Use glob patterns in `source` field (e.g., `s3://bucket/data/*.parquet`)
  - `fetch()` returns `list[Path]` for glob datasets
  - Parallel download of all matched files
  - Per-file staleness detection (only re-download changed files)
  - Cache path derivation preserves relative structure from glob matches

## [0.4.0] - 2025-12-14

### Added

- **CLI** - Full command-line interface via `catalog` command
  - `catalog init` - Initialize datacachalog in a project with example catalog
  - `catalog list` - List all datasets from discovered catalogs
  - `catalog fetch <name>` - Download a dataset with progress display
  - `catalog fetch --all` - Download all datasets in parallel
  - `catalog status` - Show cache state (cached/stale/missing) for all datasets
  - `catalog invalidate <name>` - Force re-download on next fetch
- **Catalog Discovery** - Auto-discover catalog files from `.datacachalog/catalogs/`
- **CatalogLoadError** - Graceful error handling for malformed catalog files with recovery hints

## [0.3.0] - 2025-12-14

### Added

- **Configuration Ergonomics**
  - `Catalog.from_directory()` factory - auto-discover project root and create catalog with sensible defaults
  - `Dataset.with_resolved_paths(root)` - resolve relative cache paths against project root
  - `find_project_root()` utility for locating project root via marker files
- **Write Support**
  - `catalog.push(name, local_path)` - upload local files to remote storage
  - Progress reporting for upload operations
- **Progress & Parallelism**
  - `fetch_all()` with parallel downloads and aggregate progress display
  - Per-file download progress (bytes transferred)
  - Export `RichProgressReporter` from package root
- **Error Handling**
  - Domain exceptions with `recovery_hint` property for actionable error messages
  - Exception hierarchy: `DatacachalogError`, `StorageError`, `CacheError`, `ConfigurationError`

## [0.2.0] - 2025-12-13

### Added

- **RouterStorage** - Composite adapter for URI scheme-based routing (`s3://`, `file://`, local paths)
- **S3Storage** - Boto3-based adapter for S3 bucket access with streaming downloads
- **FilesystemStorage** - Local filesystem adapter with MD5-based ETags
- **FileCache** - File-based cache with JSON metadata sidecar for staleness tracking
- **Catalog** - Core service with `fetch()`, `is_stale()`, `invalidate()`, and `datasets` property
- `create_router()` factory for convenient RouterStorage setup with sensible defaults
- `file://` URI support with automatic path stripping
- Integration tests with moto for S3 operations

## [0.1.1] - 2025-12-12

### Added

- Initial project setup with pyproject.toml, uv, ruff, mypy, pytest
- Pre-commit hooks for linting, formatting, type checking, and tests
- Core models: `Dataset`, `CacheMetadata`, `FileMetadata`
- Port definitions: `StoragePort`, `CachePort`

[0.5.0]: https://github.com/PhilHem/datacachalog/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/PhilHem/datacachalog/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/PhilHem/datacachalog/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/PhilHem/datacachalog/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/PhilHem/datacachalog/releases/tag/v0.1.1
