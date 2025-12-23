"""Pytest configuration and shared fixtures.

This module registers custom markers for CI job separation and provides
shared fixtures for the test suite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    import builtins
    from pathlib import Path

    from datacachalog.core.models import FileMetadata, ObjectVersion
    from datacachalog.core.ports import ProgressCallback, StoragePort
else:
    import builtins
    from pathlib import Path

    from datacachalog.core.models import FileMetadata, ObjectVersion
    from datacachalog.core.ports import ProgressCallback, StoragePort


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "core: Core models, ports, and services")
    config.addinivalue_line("markers", "storage: Storage adapters (s3, filesystem)")
    config.addinivalue_line("markers", "cache: File cache adapter")
    config.addinivalue_line("markers", "progress: Rich progress integration")
    config.addinivalue_line("markers", "cli: CLI tests")
    config.addinivalue_line("markers", "e2e: End-to-end tests")
    config.addinivalue_line(
        "markers", "tra: Test Responsibility Anchor (TRA) - namespace.Anchor format"
    )
    config.addinivalue_line(
        "markers",
        "tier: Test tier for CI job separation (0=instant, 1=fast, 2=standard, 3=slow, 4=manual)",
    )


@pytest.fixture
def fake_storage() -> StoragePort:
    """Reusable fake storage adapter for testing.

    Implements StoragePort with no-op methods for use in tests that need
    a storage adapter but don't require actual I/O operations.
    """

    class FakeStorage:
        def download(self, source: str, dest: Path, progress: ProgressCallback) -> None:
            pass

        def upload(
            self, local: Path, dest: str, progress: ProgressCallback | None = None
        ) -> None:
            pass

        def head(self, source: str) -> FileMetadata:
            return FileMetadata(etag="abc")

        def list(self, prefix: str, pattern: str | None = None) -> builtins.list[str]:
            return []

        def list_versions(
            self, source: str, limit: int | None = None
        ) -> builtins.list[ObjectVersion]:
            return []

        def head_version(self, source: str, version_id: str) -> FileMetadata:
            return FileMetadata(etag="abc")

        def download_version(
            self,
            source: str,
            dest: Path,
            version_id: str,
            progress: ProgressCallback,
        ) -> None:
            pass

    return FakeStorage()
