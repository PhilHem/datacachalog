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


@pytest.mark.cli
class TestCatalogStatus:
    """Tests for catalog status command."""

    def test_status_shows_missing_when_not_cached(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status shows 'missing' for datasets not in cache."""
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

        # Create data directory for cache (but don't fetch)
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert "missing" in result.output.lower()

    def test_status_shows_fresh_when_cached_and_not_stale(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status shows 'fresh' for cached datasets that match remote."""
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

        # Now check status
        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert "fresh" in result.output.lower()

    def test_status_shows_stale_when_remote_changed(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status shows 'stale' when remote file has changed since caching."""
        import time

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

        # Modify source file to make cache stale
        time.sleep(0.1)  # Ensure mtime changes
        source_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Now check status
        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert "stale" in result.output.lower()

    def test_status_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status --catalog X shows only that catalog's datasets."""
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

        result = runner.invoke(app, ["status", "--catalog", "core"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert "metrics" not in result.output


@pytest.mark.cli
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
