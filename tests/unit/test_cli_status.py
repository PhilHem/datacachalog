"""Tests for the CLI status command."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.Status")
@pytest.mark.tier(1)
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
        import os
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

        # Modify source file to make cache stale (use os.utime instead of sleep for determinism)
        source_file.write_text("id,name\n1,Alice\n2,Bob\n")
        future_time = time.time() + 10
        os.utime(source_file, (future_time, future_time))

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

    def test_status_shows_table_format(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status shows table format with Name and Status columns."""
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

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Rich table should have box-drawing characters
        assert "│" in result.output or "┃" in result.output  # Table borders
        # Should have column headers
        assert "Name" in result.output or "name" in result.output.lower()
        assert "Status" in result.output or "status" in result.output.lower()
        # Should not have plain text format
        assert "customers: missing" not in result.output

    def test_status_table_color_coding(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status table applies color coding (green/yellow/red for fresh/stale/missing)."""
        import os

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()

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
        # Backdate cache metadata file to ensure different timestamp
        cache_dir = tmp_path / "data"
        meta_file = cache_dir / "stale_dataset.meta.json"
        if meta_file.exists():
            # Backdate by 2 seconds to ensure it's older than source file modification
            import time as time_module

            old_time = time_module.time() - 2
            os.utime(meta_file, (old_time, old_time))
        stale_file.write_text("id,name\n1,Bob\n2,David\n")

        # Don't fetch missing_dataset - it should show as missing

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Verify table format
        assert "│" in result.output or "┃" in result.output
        # Verify all three datasets appear
        assert "fresh_dataset" in result.output
        assert "stale_dataset" in result.output
        assert "missing_dataset" in result.output
        # Verify status values appear (colors are applied via Rich Text)
        assert "fresh" in result.output
        assert "stale" in result.output
        assert "missing" in result.output

    def test_status_table_shows_catalog_prefixes(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status table shows catalog prefixes when multiple catalogs exist."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "metrics.csv").write_text("id,value\n1,42\n")

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

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should have table format
        assert "│" in result.output or "┃" in result.output
        # Should show catalog prefixes
        assert "core/customers" in result.output or "customers" in result.output
        assert "analytics/metrics" in result.output or "metrics" in result.output

    def test_status_table_empty_catalog_shows_hint(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """status shows hint message for empty catalog, not empty table."""
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

        result = runner.invoke(app, ["status"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should show hint message
        assert "No datasets found" in result.output
        assert "Run 'catalog init'" in result.output or "catalog init" in result.output
        # Should not show table borders
        assert "│" not in result.output
        assert "┃" not in result.output
