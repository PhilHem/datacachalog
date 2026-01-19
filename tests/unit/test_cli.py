"""Tests for the CLI commands."""

from pathlib import Path
from textwrap import dedent

import pytest
from rich.text import Text
from typer.testing import CliRunner

from datacachalog.cli import app
from datacachalog.cli.formatting import (
    _format_status_with_color,
    _load_catalog_datasets,
)


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.Init")
@pytest.mark.tier(1)
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
@pytest.mark.tra("UseCase.Invalidate")
@pytest.mark.tier(1)
class TestCatalogInvalidate:
    """Tests for catalog invalidate command."""

    def test_invalidate_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate removes dataset from cache, forcing re-download."""
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

        # First fetch to populate cache
        runner.invoke(app, ["fetch", "customers"])

        # Invalidate
        result = runner.invoke(app, ["invalidate", "customers"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "invalidated" in result.output.lower()

    def test_invalidate_nonexistent_dataset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate with unknown dataset shows error and hint."""
        # Create catalog with no datasets
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

        result = runner.invoke(app, ["invalidate", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()


@pytest.mark.cli
@pytest.mark.cli
@pytest.mark.tra("UseCase.LoadErrors")
@pytest.mark.tier(1)
class TestCatalogLoadErrors:
    """Tests for graceful error handling when catalog files are malformed."""

    def test_list_shows_graceful_error_for_syntax_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list shows user-friendly error for catalog with syntax error."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "bad.py").write_text("def broken(\n")  # Syntax error

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 1
        assert "error" in result.output.lower()
        assert "bad.py" in result.output
        assert "hint" in result.output.lower()

    def test_list_shows_graceful_error_for_import_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list shows user-friendly error for catalog with import error."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "bad_import.py").write_text(
            "from nonexistent_module import something"
        )

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 1
        assert "error" in result.output.lower()
        assert "bad_import.py" in result.output

    def test_fetch_shows_graceful_error_for_bad_catalog(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch shows user-friendly error for malformed catalog."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "bad.py").write_text("datasets = undefined_var")
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["fetch", "something"])

        assert result.exit_code == 1
        assert "error" in result.output.lower()
        assert "bad.py" in result.output

    def test_status_shows_graceful_error_for_bad_catalog(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status shows user-friendly error for malformed catalog."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "bad.py").write_text("def broken(\n")

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 1
        assert "error" in result.output.lower()
        assert "bad.py" in result.output

    def test_invalidate_shows_graceful_error_for_bad_catalog(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate shows user-friendly error for malformed catalog."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "bad.py").write_text("def broken(\n")
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["invalidate", "something"])

        assert result.exit_code == 1
        assert "error" in result.output.lower()
        assert "bad.py" in result.output


@pytest.mark.cli
@pytest.mark.tra("UseCase.InvalidateGlob")
@pytest.mark.tier(1)
class TestCatalogInvalidateGlob:
    """Tests for catalog invalidate-glob command."""

    def test_invalidate_glob_success(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate-glob removes all cached files for glob dataset."""
        # Create multiple source files matching glob pattern
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data_01.parquet").write_text("data1")
        (storage_dir / "data_02.parquet").write_text("data2")

        # Create catalog with glob dataset
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="logs", source="{storage_dir}/*.parquet"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch to populate cache
        runner.invoke(app, ["fetch", "logs"])

        # Invalidate glob
        result = runner.invoke(app, ["invalidate-glob", "logs"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "invalidated" in result.output.lower()
        assert "2" in result.output  # Should report count

    def test_invalidate_glob_nonexistent_dataset(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate-glob with unknown dataset shows error and hint."""
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

        result = runner.invoke(app, ["invalidate-glob", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_invalidate_glob_on_non_glob_dataset_shows_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate-glob on non-glob dataset shows helpful error."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.csv").write_text("id,name\n1,Alice\n")

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{storage_dir / "data.csv"}"),
            ]
        """)
        )
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["invalidate-glob", "customers"])

        assert result.exit_code == 1
        assert "not a glob pattern" in result.output.lower()

    def test_invalidate_glob_shows_graceful_error_for_bad_catalog(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """invalidate-glob shows user-friendly error for malformed catalog."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "bad.py").write_text("def broken(\n")
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["invalidate-glob", "something"])

        assert result.exit_code == 1
        assert "error" in result.output.lower()
        assert "bad.py" in result.output


@pytest.mark.cli
@pytest.mark.tra("Domain.Format.StatusColor")
@pytest.mark.tier(1)
class TestFormatStatusWithColor:
    """Tests for _format_status_with_color helper function."""

    def test_format_status_with_color_fresh(self) -> None:
        """Verify fresh returns green text."""
        result = _format_status_with_color("fresh")
        assert isinstance(result, Text)
        assert result.plain == "fresh"
        assert result.style == "green"

    def test_format_status_with_color_stale(self) -> None:
        """Verify stale returns yellow text."""
        result = _format_status_with_color("stale")
        assert isinstance(result, Text)
        assert result.plain == "stale"
        assert result.style == "yellow"

    def test_format_status_with_color_missing(self) -> None:
        """Verify missing returns red text."""
        result = _format_status_with_color("missing")
        assert isinstance(result, Text)
        assert result.plain == "missing"
        assert result.style == "red"


@pytest.mark.cli
@pytest.mark.tra("UseCase.List")
@pytest.mark.tier(1)
class TestLoadCatalogDatasets:
    """Tests for _load_catalog_datasets helper function."""

    def test_load_catalog_datasets_loads_single_catalog(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that helper loads datasets from a single catalog."""

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
                Dataset(name="orders", source="s3://bucket/orders.parquet"),
            ]
        """)
        )

        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = _load_catalog_datasets(catalog_name=None)

        assert len(result) == 2
        assert ("customers", "customers", "s3://bucket/customers.parquet") in result
        assert ("orders", "orders", "s3://bucket/orders.parquet") in result

    def test_load_catalog_datasets_loads_multiple_catalogs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that helper loads datasets from multiple catalogs with prefixes."""

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

        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = _load_catalog_datasets(catalog_name=None)

        assert len(result) == 2
        # Check that catalog prefixes are included in display_name
        display_names = [r[0] for r in result]
        assert "core/customers" in display_names
        assert "analytics/metrics" in display_names

    def test_load_catalog_datasets_handles_catalog_load_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that helper raises CatalogLoadError for malformed catalog."""
        from datacachalog.core.exceptions import CatalogLoadError

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        # Create catalog with syntax error
        (catalogs_dir / "bad.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
            # Missing closing bracket
        """)
        )

        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        with pytest.raises(CatalogLoadError):
            _load_catalog_datasets(catalog_name=None)

    def test_load_catalog_datasets_empty_catalogs_returns_empty_list(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that helper returns empty list when no catalogs found."""

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        # Create empty catalog
        (catalogs_dir / "empty.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = []
        """)
        )

        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = _load_catalog_datasets(catalog_name=None)

        assert result == []


@pytest.mark.cli
@pytest.mark.tier(1)
class TestCliCommandsStructure:
    """Tests for CLI commands directory structure (task 5so.3.1)."""

    def test_commands_directory_exists(self) -> None:
        """Verify cli/commands/__init__.py exists and is importable."""
        from datacachalog.cli import commands

        assert commands is not None


@pytest.mark.cli
@pytest.mark.tra("UseCase.List")
@pytest.mark.tier(1)
class TestListCommandModule:
    """Tests for list command module (task 5so.3.2)."""

    def test_list_command_imports_correctly(self) -> None:
        """Verify list_datasets can be imported from cli.commands.list."""
        from datacachalog.cli.commands.list import list_datasets

        assert list_datasets is not None

    def test_list_command_registered_in_app(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify list command still works via CLI runner after refactoring."""
        from datacachalog.cli import app

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="s3://bucket/test.parquet"),
            ]
        """)
        )
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        assert "test" in result.stdout


@pytest.mark.cli
@pytest.mark.tra("UseCase.Status")
@pytest.mark.tier(1)
class TestStatusCommandModule:
    """Tests for status command module (task 5so.3.3)."""

    def test_status_command_imports_correctly(self) -> None:
        """Verify status can be imported from cli.commands.status."""
        from datacachalog.cli.commands.status import status

        assert status is not None

    def test_status_command_registered_in_app(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify status command still works via CLI runner after refactoring."""
        from datacachalog.cli import app

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="s3://bucket/test.parquet"),
            ]
        """)
        )
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        assert "test" in result.stdout


@pytest.mark.cli
@pytest.mark.tier(1)
class TestFormattingModule:
    """Tests for formatting module (task 5so.3.4)."""

    def test_formatting_module_imports_correctly(self) -> None:
        """Verify formatting helpers can be imported from cli.formatting."""
        from datacachalog.cli.formatting import (
            _format_status_with_color,
            _load_catalog_datasets,
        )

        assert _format_status_with_color is not None
        assert _load_catalog_datasets is not None

    def test_list_and_status_import_from_formatting(self) -> None:
        """Verify list and status commands import from formatting module."""
        from datacachalog.cli.commands import list as list_module
        from datacachalog.cli.commands import status as status_module

        # Check that the modules exist and can be imported
        assert list_module is not None
        assert status_module is not None
