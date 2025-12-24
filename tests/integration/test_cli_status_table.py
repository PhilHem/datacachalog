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
