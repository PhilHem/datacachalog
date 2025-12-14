"""Tests for catalog discovery functionality."""

from pathlib import Path

import pytest

from datacachalog.discovery import discover_catalogs, load_catalog


@pytest.mark.core
class TestDiscoverCatalogs:
    """Tests for discover_catalogs function."""

    def test_discover_finds_catalog_files(self, tmp_path: Path) -> None:
        """discover_catalogs() finds .py files in catalogs dir."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "core.py").write_text("datasets = []")
        (catalogs_dir / "analytics.py").write_text("datasets = []")

        catalogs = discover_catalogs(tmp_path)

        assert "core" in catalogs
        assert "analytics" in catalogs
        assert catalogs["core"] == catalogs_dir / "core.py"

    def test_discover_ignores_underscore_files(self, tmp_path: Path) -> None:
        """Files starting with _ are ignored."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "core.py").write_text("datasets = []")
        (catalogs_dir / "_private.py").write_text("datasets = []")
        (catalogs_dir / "__init__.py").write_text("")

        catalogs = discover_catalogs(tmp_path)

        assert "core" in catalogs
        assert "_private" not in catalogs
        assert "__init__" not in catalogs

    def test_discover_returns_empty_when_no_catalogs_dir(self, tmp_path: Path) -> None:
        """Returns empty dict when .datacachalog/catalogs/ doesn't exist."""
        catalogs = discover_catalogs(tmp_path)
        assert catalogs == {}


@pytest.mark.core
class TestLoadCatalog:
    """Tests for load_catalog function."""

    def test_load_catalog_extracts_datasets(self, tmp_path: Path) -> None:
        """load_catalog() returns datasets list from module."""
        catalog_file = tmp_path / "test_catalog.py"
        catalog_file.write_text("""\
from datacachalog import Dataset

datasets = [
    Dataset(
        name="customers",
        source="s3://bucket/customers.parquet",
        description="Customer data",
    ),
    Dataset(
        name="orders",
        source="s3://bucket/orders.parquet",
    ),
]
""")

        datasets, cache_dir = load_catalog(catalog_file)

        assert len(datasets) == 2
        assert datasets[0].name == "customers"
        assert datasets[0].source == "s3://bucket/customers.parquet"
        assert datasets[1].name == "orders"
        assert cache_dir is None

    def test_load_catalog_extracts_cache_dir(self, tmp_path: Path) -> None:
        """load_catalog() returns cache_dir if defined."""
        catalog_file = tmp_path / "test_catalog.py"
        catalog_file.write_text("""\
from datacachalog import Dataset

datasets = []
cache_dir = "data/custom"
""")

        datasets, cache_dir = load_catalog(catalog_file)

        assert datasets == []
        assert cache_dir == "data/custom"

    def test_load_catalog_handles_missing_cache_dir(self, tmp_path: Path) -> None:
        """load_catalog() returns None for cache_dir if not defined."""
        catalog_file = tmp_path / "test_catalog.py"
        catalog_file.write_text("""\
from datacachalog import Dataset

datasets = []
""")

        _datasets, cache_dir = load_catalog(catalog_file)

        assert cache_dir is None
