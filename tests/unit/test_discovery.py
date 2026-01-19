"""Tests for catalog discovery functionality."""

from pathlib import Path

import pytest

from datacachalog.core.exceptions import (
    CatalogLoadError,
    DatacachalogError,
    UnsafeCatalogPathError,
)
from datacachalog.discovery import discover_catalogs, load_catalog


@pytest.mark.core
@pytest.mark.tra("Domain.Discovery")
@pytest.mark.tier(1)
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
@pytest.mark.tra("Domain.Discovery")
@pytest.mark.tier(1)
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

        datasets, cache_dir = load_catalog(catalog_file, catalog_root=tmp_path)

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

        datasets, cache_dir = load_catalog(catalog_file, catalog_root=tmp_path)

        assert datasets == []
        assert cache_dir == "data/custom"

    def test_load_catalog_handles_missing_cache_dir(self, tmp_path: Path) -> None:
        """load_catalog() returns None for cache_dir if not defined."""
        catalog_file = tmp_path / "test_catalog.py"
        catalog_file.write_text("""\
from datacachalog import Dataset

datasets = []
""")

        _datasets, cache_dir = load_catalog(catalog_file, catalog_root=tmp_path)

        assert cache_dir is None

    def test_load_catalog_requires_catalog_root(self, tmp_path: Path) -> None:
        """load_catalog() raises TypeError when catalog_root is not provided."""
        catalog_file = tmp_path / "test_catalog.py"
        catalog_file.write_text("datasets = []")

        with pytest.raises(TypeError) as exc_info:
            load_catalog(catalog_file)  # Missing required catalog_root

        assert "catalog_root" in str(exc_info.value)

    def test_load_catalog_accepts_explicit_catalog_root(self, tmp_path: Path) -> None:
        """load_catalog() works when catalog_root is explicitly provided."""
        catalog_root = tmp_path / "allowed"
        catalog_root.mkdir()

        catalog_file = catalog_root / "test_catalog.py"
        catalog_file.write_text("datasets = []")

        datasets, cache_dir = load_catalog(path=catalog_file, catalog_root=catalog_root)

        assert datasets == []
        assert cache_dir is None


@pytest.mark.core
@pytest.mark.tra("Domain.Discovery")
@pytest.mark.tier(1)
class TestLoadCatalogErrorHandling:
    """Tests for error handling in load_catalog."""

    def test_syntax_error_raises_catalog_load_error(self, tmp_path: Path) -> None:
        """Syntax errors are wrapped in CatalogLoadError with line number."""
        catalog_file = tmp_path / "bad.py"
        catalog_file.write_text("def broken(\n")  # Missing close paren

        with pytest.raises(CatalogLoadError) as exc_info:
            load_catalog(catalog_file, catalog_root=tmp_path)

        assert exc_info.value.catalog_path == catalog_file
        assert exc_info.value.line is not None
        assert "syntax" in str(exc_info.value).lower()

    def test_import_error_raises_catalog_load_error(self, tmp_path: Path) -> None:
        """Import errors are wrapped in CatalogLoadError."""
        catalog_file = tmp_path / "bad_import.py"
        catalog_file.write_text("from nonexistent_module import something")

        with pytest.raises(CatalogLoadError) as exc_info:
            load_catalog(catalog_file, catalog_root=tmp_path)

        assert exc_info.value.catalog_path == catalog_file
        assert "nonexistent_module" in str(exc_info.value)

    def test_name_error_raises_catalog_load_error(self, tmp_path: Path) -> None:
        """NameError from undefined variable is wrapped."""
        catalog_file = tmp_path / "bad_name.py"
        catalog_file.write_text("datasets = undefined_var")

        with pytest.raises(CatalogLoadError) as exc_info:
            load_catalog(catalog_file, catalog_root=tmp_path)

        assert exc_info.value.catalog_path == catalog_file
        assert "undefined_var" in str(exc_info.value)

    def test_value_error_raises_catalog_load_error(self, tmp_path: Path) -> None:
        """ValueError (e.g., invalid Dataset) is wrapped."""
        catalog_file = tmp_path / "bad_dataset.py"
        catalog_file.write_text("""\
from datacachalog import Dataset

# Empty name should raise ValueError in Dataset.__post_init__
datasets = [Dataset(name="", source="s3://bucket/file.parquet")]
""")

        with pytest.raises(CatalogLoadError) as exc_info:
            load_catalog(catalog_file, catalog_root=tmp_path)

        assert exc_info.value.catalog_path == catalog_file

    def test_error_preserves_cause(self, tmp_path: Path) -> None:
        """CatalogLoadError preserves the underlying exception as cause."""
        catalog_file = tmp_path / "bad.py"
        catalog_file.write_text("def broken(\n")

        with pytest.raises(CatalogLoadError) as exc_info:
            load_catalog(catalog_file, catalog_root=tmp_path)

        assert exc_info.value.cause is not None
        assert isinstance(exc_info.value.cause, SyntaxError)


@pytest.mark.core
@pytest.mark.tra("Domain.Discovery.PathValidation")
@pytest.mark.tier(1)
class TestUnsafeCatalogPathError:
    """Tests for UnsafeCatalogPathError exception."""

    def test_unsafe_catalog_path_error_inherits_from_datacachalog_error(
        self,
    ) -> None:
        """UnsafeCatalogPathError inherits from DatacachalogError."""
        error = UnsafeCatalogPathError(
            path=Path("/outside/file.py"), catalog_root=Path("/inside")
        )
        assert isinstance(error, DatacachalogError)

    def test_unsafe_catalog_path_error_stores_path_and_catalog_root(
        self, tmp_path: Path
    ) -> None:
        """UnsafeCatalogPathError stores path and catalog_root attributes."""
        path = tmp_path / "file.py"
        root = tmp_path / "root"
        error = UnsafeCatalogPathError(path=path, catalog_root=root)

        assert error.path == path
        assert error.catalog_root == root

    def test_unsafe_catalog_path_error_has_recovery_hint(self, tmp_path: Path) -> None:
        """UnsafeCatalogPathError has a recovery_hint property."""
        path = tmp_path / "outside" / "file.py"
        root = tmp_path / "inside"
        error = UnsafeCatalogPathError(path=path, catalog_root=root)

        hint = error.recovery_hint
        assert hint is not None
        assert isinstance(hint, str)
        assert len(hint) > 0


@pytest.mark.core
@pytest.mark.tra("Domain.Discovery.PathValidation")
@pytest.mark.tier(1)
class TestLoadCatalogPathValidation:
    """Tests for path containment validation in load_catalog."""

    def test_load_catalog_rejects_path_outside_catalog_root(
        self, tmp_path: Path
    ) -> None:
        """load_catalog() raises UnsafeCatalogPathError if path is outside catalog_root."""
        catalog_file = tmp_path / "test_catalog.py"
        catalog_file.write_text("datasets = []")

        catalog_root = tmp_path / "allowed"
        catalog_root.mkdir()

        with pytest.raises(UnsafeCatalogPathError) as exc_info:
            load_catalog(path=catalog_file, catalog_root=catalog_root)

        assert exc_info.value.path == catalog_file
        assert exc_info.value.catalog_root == catalog_root

    def test_load_catalog_accepts_valid_path_in_catalog_root(
        self, tmp_path: Path
    ) -> None:
        """load_catalog() accepts path contained within catalog_root."""
        catalog_root = tmp_path / "allowed"
        catalog_root.mkdir()

        catalog_file = catalog_root / "test_catalog.py"
        catalog_file.write_text("datasets = []")

        datasets, cache_dir = load_catalog(path=catalog_file, catalog_root=catalog_root)

        assert datasets == []
        assert cache_dir is None

    def test_load_catalog_rejects_path_traversal(self, tmp_path: Path) -> None:
        """load_catalog() rejects ../ traversal patterns."""
        catalog_root = tmp_path / "allowed"
        catalog_root.mkdir()

        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        catalog_file = outside_dir / "test_catalog.py"
        catalog_file.write_text("datasets = []")

        # Attempt to use path traversal
        traversal_path = catalog_root / ".." / "outside" / "test_catalog.py"

        with pytest.raises(UnsafeCatalogPathError) as exc_info:
            load_catalog(path=traversal_path, catalog_root=catalog_root)

        assert exc_info.value.catalog_root == catalog_root

    def test_load_catalog_rejects_symlink_escape(self, tmp_path: Path) -> None:
        """load_catalog() rejects symlinks pointing outside catalog_root."""
        catalog_root = tmp_path / "allowed"
        catalog_root.mkdir()

        outside_dir = tmp_path / "outside"
        outside_dir.mkdir()
        outside_file = outside_dir / "evil.py"
        outside_file.write_text("datasets = []")

        # Create symlink inside catalog_root pointing outside
        symlink = catalog_root / "link.py"
        symlink.symlink_to(outside_file)

        with pytest.raises(UnsafeCatalogPathError) as exc_info:
            load_catalog(path=symlink, catalog_root=catalog_root)

        assert exc_info.value.path == symlink
        assert exc_info.value.catalog_root == catalog_root
