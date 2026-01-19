"""Tests for the CLI list command."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.List")
@pytest.mark.tier(1)
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

    def test_list_shows_table_format(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list outputs Rich table format (not plain text)."""
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

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        # Rich table should have box-drawing characters
        assert "│" in result.output or "┃" in result.output  # Table borders
        # Should have column headers
        assert "Name" in result.output or "name" in result.output.lower()
        assert "Source" in result.output or "source" in result.output.lower()
        # Should not have plain text format
        assert "customers: s3://bucket/customers.parquet" not in result.output

    def test_list_without_status_flag_unchanged(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list without --status flag shows table format without Status column."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://bucket/customers.parquet"),
            ]
        """)
        )

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        # Should have table format
        assert "│" in result.output or "┃" in result.output  # Table borders
        assert "customers" in result.output
        assert "s3://bucket/customers.parquet" in result.output
        # Should not have Status column
        assert "Status" not in result.output

    def test_list_with_status_shows_fresh_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --status shows [fresh] when dataset is cached and not stale."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

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

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        assert "customers" in result.output
        assert "fresh" in result.output  # Status column shows "fresh" (not "[fresh]")

    def test_list_with_status_shows_stale_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --status shows [stale] when dataset is cached but stale."""
        import time

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

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

        # Modify source file to make cache stale
        time.sleep(1.1)  # Ensure different timestamp
        source_file.write_text("id,name\n1,Alice\n2,Bob\n")

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        assert "customers" in result.output
        assert "stale" in result.output  # Status column shows "stale" (not "[stale]")

    def test_list_with_status_shows_missing_state(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --status shows [missing] when dataset is not cached."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

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

        # Don't fetch - dataset should be missing

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        assert "customers" in result.output
        assert (
            "missing" in result.output
        )  # Status column shows "missing" (not "[missing]")

    def test_list_with_status_and_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --status --catalog X shows status for that catalog only."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file1 = storage_dir / "data1.csv"
        source_file1.write_text("id,name\n1,Alice\n")
        source_file2 = storage_dir / "data2.csv"
        source_file2.write_text("id,name\n1,Bob\n")

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "core.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file1}"),
            ]
        """)
        )
        (catalogs_dir / "analytics.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="metrics", source="{source_file2}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch both to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "metrics"])

        result = runner.invoke(app, ["list", "--status", "--catalog", "core"])

        assert result.exit_code == 0
        assert "customers" in result.output
        assert "fresh" in result.output  # Status column shows "fresh" (not "[fresh]")
        assert "metrics" not in result.output

    def test_list_with_status_multiple_catalogs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --status shows status with catalog prefixes for multiple catalogs."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file1 = storage_dir / "data1.csv"
        source_file1.write_text("id,name\n1,Alice\n")
        source_file2 = storage_dir / "data2.csv"
        source_file2.write_text("id,name\n1,Bob\n")

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "core.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{source_file1}"),
            ]
        """)
        )
        (catalogs_dir / "analytics.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="metrics", source="{source_file2}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch both to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "metrics"])

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        assert "core/customers" in result.output
        assert "analytics/metrics" in result.output
        assert "fresh" in result.output  # Status column shows "fresh" (not "[fresh]")

    def test_list_with_status_empty_catalog(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """list --status handles empty catalog gracefully with hint message, not error."""
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

        result = runner.invoke(app, ["list", "--status"])

        # Verify success (not error)
        assert result.exit_code == 0, "Command should succeed, not return error"
        # Verify hint message is shown
        assert "init" in result.output.lower(), "Should show hint message with 'init'"
        assert "No datasets found" in result.output, (
            "Should show 'No datasets found' message"
        )
        # Verify no error indicators
        assert "Error:" not in result.output, "Should not show error message"
        assert "Traceback" not in result.output, "Should not show traceback"
        assert "Exception" not in result.output, "Should not show exception"

    def test_list_help_shows_status_flag(self) -> None:
        """list --help shows --status flag with correct description."""
        result = runner.invoke(app, ["list", "--help"])

        assert result.exit_code == 0
        assert "--status" in result.output
        assert "Show cache state (fresh/stale/missing)" in result.output

    def test_list_with_status_shows_table_with_status_column(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify table includes Status column when --status flag is set."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.csv"
        source_file.write_text("id,name\n1,Alice\n")

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

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        # Verify table format
        assert "│" in result.output or "┃" in result.output  # Table borders
        # Verify Status column header is present
        assert "Status" in result.output
        # Verify status value is in the output
        assert "fresh" in result.output

    def test_list_table_shows_catalog_prefixes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify catalog prefixes appear in table Name column."""
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

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        # Verify table format
        assert "│" in result.output or "┃" in result.output  # Table borders
        # Verify catalog prefixes are shown in Name column
        assert "core/customers" in result.output
        assert "analytics/metrics" in result.output

    def test_list_table_empty_catalog_shows_hint(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify empty catalog shows hint message, not table."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        # Create an empty catalog (catalog exists but has no datasets)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = []
        """)
        )

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["list"])

        assert result.exit_code == 0
        # Verify hint message is present
        assert "No datasets found" in result.output
        assert "init" in result.output.lower()
        # Verify no table is shown (no table borders)
        assert "│" not in result.output
        assert "┃" not in result.output
        # Verify no table column headers
        assert "Name" not in result.output
        assert "Source" not in result.output
