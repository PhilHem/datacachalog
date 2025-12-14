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
