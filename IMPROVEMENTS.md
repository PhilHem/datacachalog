# Project Improvement Recommendations

This document outlines high-impact, low-effort improvements for the datacachalog project. Each item can be tackled independently.

## 3. Rich Table Formatting for List/Status

**Status**: New recommendation

**Description**: Replace plain text output with Rich tables for `list` and `status` commands. Provides better readability with columns, colors, and alignment.

**Current State**:

- Commands output plain text lines
- Rich is already a dependency (used for progress bars)
- No visual structure to output

**Proposed Behavior**:

```bash
$ catalog list
┌─────────────┬─────────────────────────────────────┐
│ Name        │ Source                              │
├─────────────┼─────────────────────────────────────┤
│ customers   │ s3://bucket/customers.parquet       │
│ orders      │ s3://bucket/orders.parquet          │
└─────────────┴─────────────────────────────────────┘

$ catalog status
┌─────────────┬─────────┐
│ Name        │ Status  │
├─────────────┼─────────┤
│ customers   │ fresh   │
│ orders      │ stale   │
│ products    │ missing │
└─────────────┴─────────┘
```

**Effort**: ~1 hour

- Import Rich `Table` class
- Refactor `list_datasets()` and `status()` to build tables
- Add color coding (green=fresh, yellow=stale, red=missing)

**Value**: Medium-High - Significantly improves readability and professional appearance

---

## 4. `catalog sync` Command

**Status**: New recommendation

**Description**: Add a `catalog sync` command that fetches only stale datasets (not fresh ones). More efficient than `fetch --all` when most datasets are already up-to-date.

**Current State**:

- `catalog fetch --all` downloads all datasets regardless of staleness
- `Catalog.fetch_all()` exists but doesn't check staleness first
- Users must manually check status then fetch individual stale datasets

**Proposed Behavior**:

```bash
$ catalog sync
Checking 10 datasets...
Found 3 stale datasets: orders, products, transactions
Fetching stale datasets...
[Progress bars for 3 downloads]
Synced 3 datasets
```

**Effort**: ~1 hour

- Add new `sync()` CLI command
- Use `Catalog.is_stale()` to filter datasets
- Call `Catalog.fetch()` for each stale dataset
- Show summary of what was synced

**Value**: High - Common workflow improvement, saves time and bandwidth

---

## 5. Retry Logic for Transient Failures

**Status**: Already mentioned in ROADMAP.md (Future section)

**Description**: Add exponential backoff retry logic for network operations (S3 downloads/uploads, head requests). Handles transient failures gracefully without requiring user intervention.

**Current State**:

- Network failures immediately raise exceptions
- No retry mechanism for transient errors (timeouts, 503s, etc.)
- Users must manually retry failed operations

**Proposed Behavior**:

- Automatically retry failed operations with exponential backoff
- Configurable retry count (default: 3 attempts)
- Only retry on transient errors (network timeouts, 503, 502, etc.)
- Log retry attempts for debugging

**Implementation Notes**:

- Add retry logic in storage adapters (`S3Storage`, `FilesystemStorage`)
- Use `tenacity` library or implement simple exponential backoff
- Retry on: `ClientError` with specific error codes, `ReadTimeout`, `ConnectTimeout`
- Don't retry on: `404 Not Found`, `403 Forbidden` (permanent errors)

**Effort**: ~2-3 hours

- Research retry patterns for boto3
- Implement retry decorator or wrapper
- Add configuration for retry count/backoff
- Write tests for retry scenarios

**Value**: High - Critical for production reliability, handles common failure scenarios

---

## 6. Validation/Health Check Command

**Status**: New recommendation

**Description**: Add a `catalog validate` command that performs comprehensive health checks on the catalog: dataset accessibility, storage connectivity, cache integrity, and configuration validity.

**Current State**:

- No way to verify catalog health without manual testing
- Errors only surface when fetching specific datasets
- No CI/CD integration for catalog validation

**Proposed Behavior**:

```bash
$ catalog validate
Validating catalog...
✓ Storage connectivity: OK
✓ Cache integrity: OK
✓ Dataset 'customers': Accessible (fresh)
✓ Dataset 'orders': Accessible (stale)
✗ Dataset 'products': Not found (s3://bucket/products.parquet)
✗ Dataset 'transactions': Permission denied

Validation complete: 2 errors found
```

**Checks to Perform**:

1. Storage connectivity (test head() on a known file or bucket)
2. Cache directory exists and is writable
3. Each dataset:
   - Source is accessible (head() succeeds)
   - Cache metadata is valid (if cached)
   - Cache file exists (if metadata says it should)
4. Configuration validity (no duplicate names, valid URIs)

**Effort**: ~2-3 hours

- Add new `validate()` CLI command
- Implement health check logic in `Catalog` class
- Create structured validation results
- Format output with Rich (green checkmarks, red X marks)

**Value**: High - Essential for CI/CD, troubleshooting, and ensuring catalog reliability

---

## Summary

| Improvement             | Status      | Effort  | Value       | Priority |
| ----------------------- | ----------- | ------- | ----------- | -------- |
| `catalog list --status` | ✅ Complete | 30 min  | High        | ⭐⭐⭐   |
| Cache Statistics CLI    | ✅ Complete | 1-2 hrs | High        | ⭐⭐⭐   |
| Rich Table Formatting   | Not started | 1 hr    | Medium-High | ⭐⭐     |
| `catalog sync`          | Not started | 1 hr    | High        | ⭐⭐⭐   |
| Retry Logic             | Not started | 2-3 hrs | High        | ⭐⭐⭐   |
| Validation Command      | Not started | 2-3 hrs | High        | ⭐⭐⭐   |

**Note**: Cache statistics infrastructure exists (`cache_size()`, `FileCache.size()`, `_format_size()`), but no dedicated CLI command. The `catalog clean` command already exists (beads issue `datacachalog-wvn` closed).

**Total Estimated Effort**: ~8-11 hours for all improvements

**Recommended Order**:

1. Start with quick wins: `list --status` and Rich tables (1.5 hrs)
2. Add `sync` command (1 hr) - builds on existing functionality
3. Add cache statistics (1-2 hrs) - exposes existing methods
4. Implement retry logic (2-3 hrs) - production reliability
5. Add validation command (2-3 hrs) - comprehensive health checks
