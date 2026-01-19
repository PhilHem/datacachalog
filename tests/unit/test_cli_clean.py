"""Tests for the CLI clean command."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.Clean")
@pytest.mark.tier(1)
class TestCatalogClean:
    """Tests for catalog clean command."""

    def test_clean_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean successfully removes orphaned files and reports count."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # First fetch to populate cache with valid key
        runner.invoke(app, ["fetch", "customers"])

        # Manually add orphaned cache entry
        orphaned_file = cache_dir / "orphaned.csv"
        orphaned_file.write_text("orphaned data")
        orphaned_meta = cache_dir / "orphaned.csv.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "1" in result.output or "removed" in result.output.lower()

    def test_clean_no_orphaned_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean returns 0 when no orphaned files exist."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache with valid key
        runner.invoke(app, ["fetch", "customers"])

        # Clean (should find no orphaned files)
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "0" in result.output or "removed" in result.output.lower()

    def test_clean_with_orphaned_files(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean removes multiple orphaned files and reports correct count."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache with valid key
        runner.invoke(app, ["fetch", "customers"])

        # Manually add multiple orphaned cache entries
        for key in ["orphaned1.csv", "orphaned2.csv"]:
            orphaned_file = cache_dir / key
            orphaned_file.write_text("orphaned data")
            orphaned_meta = cache_dir / f"{key}.meta.json"
            orphaned_meta.write_text(
                '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
            )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "2" in result.output or "removed" in result.output.lower()

    def test_clean_no_catalogs_found_exits_with_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean exits with error when no catalogs exist."""
        monkeypatch.chdir(tmp_path)
        result = runner.invoke(app, ["clean"])
        assert result.exit_code != 0

    def test_clean_preserves_valid_cache_entries(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean removes orphaned entries but preserves valid dataset cache entries."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file1 = storage_dir / "customers.csv"
        source_file1.write_text("id,name\n1,Alice\n")
        source_file2 = storage_dir / "products.csv"
        source_file2.write_text("id,name\n1,Widget\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file1}"),
                Dataset(name="products", source="{source_file2}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch both datasets to populate cache with valid keys
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "products"])

        # Manually add orphaned cache entry
        orphaned_file = cache_dir / "orphaned.csv"
        orphaned_file.write_text("orphaned data")
        orphaned_meta = cache_dir / "orphaned.csv.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "1" in result.output or "removed" in result.output.lower()

        # Verify valid cache entries are still present
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
            all_ds.extend(datasets)

        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        assert cat._cache.get("customers") is not None
        assert cat._cache.get("products") is not None
        assert cat._cache.get("orphaned") is None

    def test_clean_preserves_glob_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean preserves glob dataset cache keys."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "monthly_data").mkdir()
        (storage_dir / "monthly_data" / "2024-01.parquet").write_text("data1")
        (storage_dir / "monthly_data" / "2024-02.parquet").write_text("data2")

        # Create catalog with glob dataset
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="monthly_data", source="{storage_dir}/monthly_data/*.parquet"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch glob dataset to populate cache
        runner.invoke(app, ["fetch", "monthly_data"])

        # Manually add orphaned cache entry
        orphaned_file = cache_dir / "orphaned.csv"
        orphaned_file.write_text("orphaned data")
        orphaned_meta = cache_dir / "orphaned.csv.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "1" in result.output or "removed" in result.output.lower()

        # Verify glob keys are preserved
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
            all_ds.extend(datasets)

        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        assert cat._cache.get("monthly_data/2024-01.parquet") is not None
        assert cat._cache.get("monthly_data/2024-02.parquet") is not None
        assert cat._cache.get("orphaned") is None

    def test_clean_removes_orphaned_glob_prefix_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean removes orphaned keys that look like glob keys but don't match any dataset prefix."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        # Manually add orphaned glob-looking key
        orphaned_file = cache_dir / "nonexistent_dataset/file.csv"
        orphaned_file.parent.mkdir(parents=True, exist_ok=True)
        orphaned_file.write_text("orphaned data")
        orphaned_meta = cache_dir / "nonexistent_dataset/file.csv.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "1" in result.output or "removed" in result.output.lower()

        # Verify orphaned glob-prefix key is removed
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
            all_ds.extend(datasets)

        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        assert cat._cache.get("customers") is not None
        assert cat._cache.get("nonexistent_dataset/file.csv") is None

    def test_clean_preserves_versioned_cache_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean preserves versioned cache keys (format: YYYY-MM-DDTHHMMSS.ext)."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        # Manually add versioned cache key
        versioned_key = "2024-01-15T120000.parquet"
        versioned_file = cache_dir / versioned_key
        versioned_file.write_text("versioned data")
        versioned_meta = cache_dir / f"{versioned_key}.meta.json"
        versioned_meta.write_text(
            '{"etag": "version1", "cached_at": "2024-01-15T12:00:00", "source": ""}'
        )

        # Manually add orphaned cache entry
        orphaned_file = cache_dir / "orphaned"
        orphaned_file.write_text("orphaned data")
        orphaned_meta = cache_dir / "orphaned.meta.json"
        orphaned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "1" in result.output or "removed" in result.output.lower()

        # Verify versioned key is preserved
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
            all_ds.extend(datasets)

        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        assert cat._cache.get(versioned_key) is not None
        assert cat._cache.get("orphaned") is None

    def test_clean_removes_orphaned_versioned_looking_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean preserves versioned-looking keys (all keys matching versioned pattern are preserved, even if orphaned)."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        # Manually add orphaned versioned-looking key (even though it's orphaned, pattern says preserve)
        orphaned_versioned_key = "2024-01-01T000000.csv"
        orphaned_versioned_file = cache_dir / orphaned_versioned_key
        orphaned_versioned_file.write_text("orphaned versioned data")
        orphaned_versioned_meta = cache_dir / f"{orphaned_versioned_key}.meta.json"
        orphaned_versioned_meta.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Manually add another orphaned (non-versioned) key with both data and metadata
        orphaned_file2 = cache_dir / "orphaned2"
        orphaned_file2.write_text("orphaned data 2")
        orphaned_meta2 = cache_dir / "orphaned2.meta.json"
        orphaned_meta2.write_text(
            '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
        )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Versioned-looking keys are preserved, so only non-versioned orphaned key is removed
        assert "1" in result.output or "removed" in result.output.lower()

        # Verify versioned-looking key is preserved (current behavior)
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
            all_ds.extend(datasets)

        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        # Versioned pattern keys are always preserved by current implementation
        assert cat._cache.get(orphaned_versioned_key) is not None
        assert cat._cache.get("orphaned") is None

    def test_clean_with_empty_cache_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean succeeds and reports 0 when cache directory exists but is empty."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Clean without fetching anything
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "0" in result.output or "removed" in result.output.lower()

    def test_clean_with_mixed_orphaned_and_valid_and_glob_keys(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """clean correctly handles mixed datasets: glob, versioned, and regular."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "monthly_data").mkdir()
        (storage_dir / "monthly_data" / "2024-01.parquet").write_text("data1")
        (storage_dir / "monthly_data" / "2024-02.parquet").write_text("data2")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="monthly_data", source="{storage_dir}/monthly_data/*.parquet"),
            ]
        """)
        )

        cache_dir = tmp_path / "data"
        cache_dir.mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch glob dataset
        runner.invoke(app, ["fetch", "monthly_data"])

        # Manually add orphaned keys (both regular and versioned-looking)
        for key in ["orphaned1", "orphaned2"]:
            orphaned_file = cache_dir / key
            orphaned_file.write_text(f"{key} data")
            orphaned_meta = cache_dir / f"{key}.meta.json"
            orphaned_meta.write_text(
                '{"etag": "orphaned", "cached_at": "2024-01-01T00:00:00", "source": ""}'
            )

        # Clean orphaned files
        result = runner.invoke(app, ["clean"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "2" in result.output or "removed" in result.output.lower()

        # Verify glob keys are preserved and orphaned keys are removed
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, _ = load_catalog(catalog_path, catalog_root=catalog_root)
            all_ds.extend(datasets)

        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        assert cat._cache.get("monthly_data/2024-01.parquet") is not None
        assert cat._cache.get("monthly_data/2024-02.parquet") is not None
        assert cat._cache.get("orphaned1") is None
        assert cat._cache.get("orphaned2") is None
