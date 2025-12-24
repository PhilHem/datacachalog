"""Integration tests for list --status CLI command.

These tests verify the list command with --status flag works correctly
with filesystem adapter across different scenarios.
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
@pytest.mark.tra("UseCase.List")
@pytest.mark.tier(1)
class TestListStatusIntegration:
    """Integration tests for list --status command."""

    def test_list_status_integration_fresh_stale_missing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify list --status shows fresh, stale, and missing states correctly."""
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

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        assert "fresh_dataset" in result.output
        assert "[fresh]" in result.output
        assert "stale_dataset" in result.output
        assert "[stale]" in result.output
        assert "missing_dataset" in result.output
        assert "[missing]" in result.output

    def test_list_status_integration_multiple_catalogs(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify list --status works correctly with multiple catalogs."""
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

        result = runner.invoke(app, ["list", "--status"])

        assert result.exit_code == 0
        # Should show catalog prefixes
        assert "core/customers" in result.output or "customers" in result.output
        assert "[fresh]" in result.output
        assert "analytics/metrics" in result.output or "metrics" in result.output
        # Both should show fresh status
        assert result.output.count("[fresh]") == 2

    def test_list_status_integration_catalog_filter(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify list --status --catalog X shows status only for that catalog."""
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
        result = runner.invoke(app, ["list", "--status", "--catalog", "core"])

        assert result.exit_code == 0
        assert "customers" in result.output
        assert "[fresh]" in result.output
        # Should NOT show metrics from analytics catalog
        assert "metrics" not in result.output or "analytics" not in result.output

        # Test with --catalog analytics
        result2 = runner.invoke(app, ["list", "--status", "--catalog", "analytics"])

        assert result2.exit_code == 0
        assert "metrics" in result2.output
        assert "[fresh]" in result2.output
        # Should NOT show customers from core catalog
        assert "customers" not in result2.output or "core" not in result2.output
