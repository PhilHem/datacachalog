"""Pytest configuration and shared fixtures.

This module registers custom markers for CI job separation and provides
shared fixtures for the test suite.
"""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line("markers", "core: Core models, ports, and services")
    config.addinivalue_line("markers", "storage: Storage adapters (s3, filesystem)")
    config.addinivalue_line("markers", "cache: File cache adapter")
    config.addinivalue_line("markers", "progress: Rich progress integration")
    config.addinivalue_line("markers", "cli: CLI tests")
    config.addinivalue_line("markers", "e2e: End-to-end tests")
