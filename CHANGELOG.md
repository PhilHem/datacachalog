# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.2.0]: https://github.com/PhilHem/datacachalog/compare/v0.1.1...v0.2.0
[0.1.1]: https://github.com/PhilHem/datacachalog/releases/tag/v0.1.1
