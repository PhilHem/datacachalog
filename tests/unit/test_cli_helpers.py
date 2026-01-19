"""Tests for CLI helper functions and commands."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tier(1)
class TestLoadCatalogContext:
    """Tests for load_catalog_context helper function."""

    def test_load_catalog_context_without_filter_returns_all_catalogs(
        self, tmp_path: Path
    ) -> None:
        """load_catalog_context() returns all catalogs when no filter specified."""
        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure with multiple catalogs
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        # Create two catalog files
        (catalogs_dir / "default.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
            ]
            """
            )
        )
        (catalogs_dir / "core.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="orders", source="s3://bucket/orders.parquet"),
            ]
            """
            )
        )

        # Change to tmp_path to simulate running from project root
        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            catalog, root, catalogs = load_catalog_context()

            assert catalog is not None
            assert root == tmp_path
            assert len(catalogs) == 2
            assert "default" in catalogs
            assert "core" in catalogs
            # Should have datasets from both catalogs
            assert len(catalog.datasets) == 2
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_with_filter_returns_single_catalog(
        self, tmp_path: Path
    ) -> None:
        """load_catalog_context() filters to single catalog when name specified."""
        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure with multiple catalogs
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "default.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
            ]
            """
            )
        )
        (catalogs_dir / "core.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="orders", source="s3://bucket/orders.parquet"),
            ]
            """
            )
        )

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            catalog, root, catalogs = load_catalog_context(catalog_name="default")

            assert catalog is not None
            assert root == tmp_path
            assert len(catalogs) == 1
            assert "default" in catalogs
            # Should only have datasets from default catalog
            assert len(catalog.datasets) == 1
            assert catalog.datasets[0].name == "customers"
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_raises_when_catalog_not_found(
        self, tmp_path: Path
    ) -> None:
        """load_catalog_context() raises typer.Exit when catalog name doesn't exist."""
        import typer

        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure with one catalog
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "default.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = []
            """
            )
        )

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            with pytest.raises(typer.Exit) as exc_info:
                load_catalog_context(catalog_name="nonexistent")
            assert exc_info.value.exit_code == 1
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_handles_load_errors(self, tmp_path: Path) -> None:
        """load_catalog_context() handles CatalogLoadError properly."""
        import typer

        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure with invalid catalog
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        # Create catalog with syntax error
        (catalogs_dir / "broken.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="s3://bucket/test.parquet"
                # Missing closing parenthesis
            ]
            """
            )
        )

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            with pytest.raises(typer.Exit) as exc_info:
                load_catalog_context(catalog_name="broken")
            assert exc_info.value.exit_code == 1
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_returns_catalog_instance(
        self, tmp_path: Path
    ) -> None:
        """load_catalog_context() returns functional Catalog instance."""
        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        source_file = tmp_path / "source.txt"
        source_file.write_text("test content")

        (catalogs_dir / "default.py").write_text(
            dedent(
                f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="{source_file}"),
            ]
            """
            )
        )

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            catalog, _root, _catalogs = load_catalog_context()

            assert catalog is not None
            # Should be able to use the catalog
            assert len(catalog.datasets) == 1
            assert catalog.datasets[0].name == "test"
            # Should be able to fetch
            result = catalog.fetch("test")
            assert result is not None
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_uses_cache_dir_from_catalog(
        self, tmp_path: Path
    ) -> None:
        """load_catalog_context() respects cache_dir from catalog file."""
        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "default.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = []
            cache_dir = "custom_cache"
            """
            )
        )

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            catalog, _root, _catalogs = load_catalog_context()

            assert catalog is not None
            # Should use custom cache_dir
            assert catalog._cache_dir == tmp_path / "custom_cache"
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_defaults_cache_dir_when_not_specified(
        self, tmp_path: Path
    ) -> None:
        """load_catalog_context() defaults to 'data' when cache_dir not in catalog."""
        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "default.py").write_text(
            dedent(
                """\
            from datacachalog import Dataset
            datasets = []
            """
            )
        )

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            catalog, _root, _catalogs = load_catalog_context()

            assert catalog is not None
            # Should default to "data"
            assert catalog._cache_dir == tmp_path / "data"
        finally:
            os.chdir(original_cwd)

    def test_load_catalog_context_handles_empty_catalogs(self, tmp_path: Path) -> None:
        """load_catalog_context() handles case when no catalogs exist."""
        import typer

        from datacachalog.cli.main import load_catalog_context

        # Setup: create project structure but no catalogs
        (tmp_path / ".git").mkdir()
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        import os

        original_cwd = Path.cwd()
        try:
            os.chdir(tmp_path)
            # Should raise typer.Exit when no catalogs found
            with pytest.raises(typer.Exit) as exc_info:
                load_catalog_context()
            assert exc_info.value.exit_code == 1
        finally:
            os.chdir(original_cwd)


@pytest.mark.cli
@pytest.mark.tra("UseCase.CacheStats")
@pytest.mark.tier(1)
class TestCacheStats:
    """Tests for catalog cache-stats command."""

    def test_cache_stats_shows_total_size(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats shows total cache size correctly."""
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

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "Total cache size:" in result.output

    def test_cache_stats_shows_entry_count(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats shows total entry count matching cached datasets."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "orders.csv").write_text("id,amount\n1,100\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{storage_dir / "customers.csv"}"),
                Dataset(name="orders", source="{storage_dir / "orders.csv"}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch both to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "orders"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "Total entries:" in result.output
        assert "2" in result.output  # Should show 2 entries

    def test_cache_stats_shows_cache_directory(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats shows cache directory path."""
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

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "Cache directory:" in result.output
        assert "data" in result.output

    def test_cache_stats_shows_per_dataset_breakdown(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats shows per-dataset breakdown format."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "orders.csv").write_text("id,amount\n1,100\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{storage_dir / "customers.csv"}"),
                Dataset(name="orders", source="{storage_dir / "orders.csv"}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "orders"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "Per-dataset breakdown:" in result.output
        assert "customers:" in result.output
        assert "orders:" in result.output

    def test_cache_stats_shows_freshness_status(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats shows fresh/stale status correctly."""
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

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers:" in result.output
        assert "(fresh)" in result.output or "(stale)" in result.output

    def test_cache_stats_with_empty_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats handles empty cache correctly."""
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

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Don't fetch - cache should be empty
        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "Total cache size:" in result.output
        assert "Total entries:" in result.output
        assert "0" in result.output  # Should show 0 entries

    def test_cache_stats_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats --catalog X shows only that catalog's datasets."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "metrics.csv").write_text("id,value\n1,42\n")

        # Create two catalogs
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "core.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{storage_dir / "customers.csv"}"),
            ]
        """)
        )

        (catalogs_dir / "analytics.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="metrics", source="{storage_dir / "metrics.csv"}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch both to populate cache
        runner.invoke(app, ["fetch", "customers", "--catalog", "core"])
        runner.invoke(app, ["fetch", "metrics", "--catalog", "analytics"])

        result = runner.invoke(app, ["cache-stats", "--catalog", "core"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers:" in result.output
        assert "metrics:" not in result.output

    def test_cache_stats_with_no_datasets(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats handles empty catalog correctly."""
        # Create empty catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = []
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should show appropriate message or empty breakdown
        assert (
            "Total cache size:" in result.output
            or "No datasets" in result.output.lower()
        )
