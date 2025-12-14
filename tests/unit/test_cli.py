"""Tests for the CLI commands."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
class TestCatalogInit:
    """Tests for catalog init command."""

    def test_init_creates_catalog_dir(self, tmp_path: Path) -> None:
        """init creates .datacachalog/catalogs/ structure."""
        result = runner.invoke(app, ["init", str(tmp_path)])

        assert result.exit_code == 0
        assert (tmp_path / ".datacachalog" / "catalogs").is_dir()

    def test_init_creates_default_catalog(self, tmp_path: Path) -> None:
        """init creates default.py with example template."""
        runner.invoke(app, ["init", str(tmp_path)])

        default_py = tmp_path / ".datacachalog" / "catalogs" / "default.py"
        assert default_py.exists()

        content = default_py.read_text()
        assert "from datacachalog import Dataset" in content
        assert "datasets = [" in content

    def test_init_creates_default_data_dirs(self, tmp_path: Path) -> None:
        """init creates 01_raw, 02_intermediate, 03_processed, 04_output."""
        runner.invoke(app, ["init", str(tmp_path)])

        data_dir = tmp_path / "data"
        assert data_dir.is_dir()
        assert (data_dir / "01_raw").is_dir()
        assert (data_dir / "02_intermediate").is_dir()
        assert (data_dir / "03_processed").is_dir()
        assert (data_dir / "04_output").is_dir()

    def test_init_custom_dirs(self, tmp_path: Path) -> None:
        """--dirs 'raw,staging,gold' creates custom directories."""
        runner.invoke(app, ["init", str(tmp_path), "--dirs", "raw,staging,gold"])

        data_dir = tmp_path / "data"
        assert (data_dir / "raw").is_dir()
        assert (data_dir / "staging").is_dir()
        assert (data_dir / "gold").is_dir()
        # Should NOT have default dirs
        assert not (data_dir / "01_raw").exists()

    def test_init_numbered_custom_dirs(self, tmp_path: Path) -> None:
        """--dirs 'raw,staging' --numbered creates 01_raw, 02_staging."""
        runner.invoke(
            app, ["init", str(tmp_path), "--dirs", "raw,staging", "--numbered"]
        )

        data_dir = tmp_path / "data"
        assert (data_dir / "01_raw").is_dir()
        assert (data_dir / "02_staging").is_dir()

    def test_init_flat(self, tmp_path: Path) -> None:
        """--flat creates just data/ with no subdirectories."""
        runner.invoke(app, ["init", str(tmp_path), "--flat"])

        data_dir = tmp_path / "data"
        assert data_dir.is_dir()
        # Should have no subdirectories
        subdirs = [p for p in data_dir.iterdir() if p.is_dir()]
        assert subdirs == []

    def test_init_is_idempotent(self, tmp_path: Path) -> None:
        """init doesn't overwrite existing files."""
        # First init
        runner.invoke(app, ["init", str(tmp_path)])

        # Modify the default.py
        default_py = tmp_path / ".datacachalog" / "catalogs" / "default.py"
        original_content = default_py.read_text()
        custom_content = original_content + "\n# Custom modification\n"
        default_py.write_text(custom_content)

        # Second init
        result = runner.invoke(app, ["init", str(tmp_path)])

        # Should succeed but not overwrite
        assert result.exit_code == 0
        assert default_py.read_text() == custom_content

    def test_init_shows_created_paths(self, tmp_path: Path) -> None:
        """init shows what was created in output."""
        result = runner.invoke(app, ["init", str(tmp_path)])

        assert "Created" in result.output or "created" in result.output


@pytest.mark.cli
class TestCatalogList:
    """Tests for catalog list command."""

    def test_list_shows_all_datasets_merged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list shows datasets from all catalogs with prefixes."""
        # Create catalog structure
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        # Create two catalogs with datasets
        (catalogs_dir / "core.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
                Dataset(name="orders", source="s3://bucket/orders.parquet"),
            ]
        """)
        )

        (catalogs_dir / "analytics.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="metrics", source="s3://bucket/metrics.parquet"),
            ]
        """)
        )

        # Change to tmp_path so CLI discovers catalogs there
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        assert "core/customers" in result.output
        assert "core/orders" in result.output
        assert "analytics/metrics" in result.output

    def test_list_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --catalog X shows only that catalog's datasets."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "core.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
            ]
        """)
        )

        (catalogs_dir / "analytics.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="metrics", source="s3://bucket/metrics.parquet"),
            ]
        """)
        )

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list", "--catalog", "core"])

        assert result.exit_code == 0
        assert "customers" in result.output
        assert "metrics" not in result.output

    def test_list_empty_shows_hint(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list with no datasets suggests 'catalog init'."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert "init" in result.output.lower()


@pytest.mark.cli
class TestCatalogFetch:
    """Tests for catalog fetch command."""

    def test_fetch_returns_cached_path(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch downloads dataset and outputs the cached path."""
        # Create source file (simulates remote storage)
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create catalog with dataset pointing to source file
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

        # Create data directory for cache
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["fetch", "customers"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Output should contain the path to cached file
        assert "data" in result.output

    def test_fetch_dataset_not_found_exits_with_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch with unknown dataset name shows error and exits 1."""
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

        result = runner.invoke(app, ["fetch", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_fetch_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch --catalog X fetches from that specific catalog."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

        # Create two catalogs
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "core.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file}"),
            ]
        """)
        )

        (catalogs_dir / "analytics.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="metrics", source="s3://nonexistent/metrics.parquet"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch from core catalog specifically
        result = runner.invoke(app, ["fetch", "customers", "--catalog", "core"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "data" in result.output

    def test_fetch_with_progress_does_not_crash(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch displays progress without crashing (progress is opt-in)."""
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

        # Fetch should work with progress enabled (Rich may not render in test runner)
        result = runner.invoke(app, ["fetch", "customers"])

        assert result.exit_code == 0, f"Failed with: {result.output}"

    def test_fetch_all_downloads_all_datasets(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch --all downloads all datasets and outputs all paths."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "orders.csv").write_text("id,amount\n1,100\n")

        # Create catalog with multiple datasets
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

        result = runner.invoke(app, ["fetch", "--all"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Both dataset paths should be in output
        assert "customers" in result.output
        assert "orders" in result.output

    def test_fetch_all_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch --all --catalog X fetches only datasets from that catalog."""
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

        result = runner.invoke(app, ["fetch", "--all", "--catalog", "core"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Only core catalog datasets
        assert "customers" in result.output
        assert "metrics" not in result.output
