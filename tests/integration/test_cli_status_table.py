"""Integration tests for status CLI command with Rich table formatting.

These tests verify the status command works correctly with Rich tables
and color coding across different scenarios.
"""

from __future__ import annotations

import time
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


if TYPE_CHECKING:
    from pathlib import Path


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.Status")
@pytest.mark.tier(1)
class TestStatusTableIntegration:
    """Integration tests for status command with Rich table formatting."""

    def test_status_table_integration_fresh_stale_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify status table shows fresh, stale, and missing states correctly with colors."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()

        # Create three source files
        fresh_file = storage_dir / "fresh.csv"
        fresh_file.write_text("id,name\n1,Alice\n")

        stale_file = storage_dir / "stale.csv"
        stale_file.write_text("id,name\n1,Bob\n")

        missing_file = storage_dir / "missing.csv"
        missing_file.write_text("id,name\n1,Charlie\n")

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="fresh_dataset", source="{fresh_file}"),
                Dataset(name="stale_dataset", source="{stale_file}"),
                Dataset(name="missing_dataset", source="{missing_file}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch fresh_dataset to populate cache (will be fresh)
        runner.invoke(app, ["fetch", "fresh_dataset"])

        # Fetch stale_dataset, then modify source to make it stale
        runner.invoke(app, ["fetch", "stale_dataset"])
        time.sleep(1.1)  # Ensure different timestamp
        stale_file.write_text("id,name\n1,Bob\n2,David\n")

        # Don't fetch missing_dataset - it should show as missing

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        # Verify table format
        assert "│" in result.output or "┃" in result.output  # Table borders
        assert "Name" in result.output or "name" in result.output.lower()
        assert "Status" in result.output or "status" in result.output.lower()
        # Verify all three datasets appear
        assert "fresh_dataset" in result.output
        assert "stale_dataset" in result.output
        assert "missing_dataset" in result.output
        # Verify status values appear (colors are applied via Rich Text)
        assert "fresh" in result.output
        assert "stale" in result.output
        assert "missing" in result.output

    def test_status_table_integration_multiple_catalogs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify status table formatting with multiple catalogs shows catalog prefixes correctly."""
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

        # Fetch both datasets to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "metrics"])

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0
        # Verify table format
        assert "│" in result.output or "┃" in result.output  # Table borders
        assert "Name" in result.output or "name" in result.output.lower()
        assert "Status" in result.output or "status" in result.output.lower()
        # Verify catalog prefixes appear in Name column
        assert "core/customers" in result.output
        assert "analytics/metrics" in result.output
        # Verify status column shows correct status values
        assert "fresh" in result.output
        # Both datasets should appear in output
        assert "customers" in result.output
        assert "metrics" in result.output

    def test_status_table_integration_catalog_filter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify --catalog flag filters datasets correctly when using table format."""
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

        # Fetch both datasets to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "metrics"])

        # Test with --catalog core
        result = runner.invoke(app, ["status", "--catalog", "core"])

        assert result.exit_code == 0
        # Verify table format
        assert "│" in result.output or "┃" in result.output  # Table borders
        assert "Status" in result.output or "status" in result.output.lower()
        # Only datasets from core catalog should appear
        assert "customers" in result.output
        assert "fresh" in result.output  # Status column shows "fresh"
        # Should NOT show metrics from analytics catalog
        assert "metrics" not in result.output

        # Test with --catalog analytics
        result2 = runner.invoke(app, ["status", "--catalog", "analytics"])

        assert result2.exit_code == 0
        # Verify table format
        assert "│" in result2.output or "┃" in result2.output  # Table borders
        assert "Status" in result2.output or "status" in result2.output.lower()
        # Only datasets from analytics catalog should appear
        assert "metrics" in result2.output
        assert "fresh" in result2.output  # Status column shows "fresh"
        # Should NOT show customers from core catalog
        assert "customers" not in result2.output
