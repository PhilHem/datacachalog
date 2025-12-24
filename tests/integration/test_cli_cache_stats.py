"""Integration tests for cache-stats CLI command.

These tests verify the cache-stats command works correctly
with filesystem and S3 storage backends across different scenarios.
"""

from __future__ import annotations

from textwrap import dedent
from typing import TYPE_CHECKING, Any

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


if TYPE_CHECKING:
    from pathlib import Path


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.CacheStats")
@pytest.mark.tier(2)
class TestCacheStatsIntegration:
    """Integration tests for cache-stats command."""

    def test_cache_stats_with_filesystem_storage(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify cache-stats works correctly with FilesystemStorage."""
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

        # Fetch datasets to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "orders"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0
        assert "Total cache size:" in result.output
        assert "Total entries:" in result.output
        assert "Cache directory:" in result.output
        assert "Per-dataset breakdown:" in result.output
        assert "customers:" in result.output
        assert "orders:" in result.output
        # Verify entries count matches
        assert "2" in result.output  # Should show 2 entries

    @pytest.mark.storage
    def test_cache_stats_with_s3_storage(
        self, s3_client: Any, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify cache-stats works correctly with S3Storage."""
        # Setup S3 files
        s3_client.put_object(
            Bucket="test-bucket", Key="customers.csv", Body=b"id,name\n1,Alice\n"
        )
        s3_client.put_object(
            Bucket="test-bucket", Key="orders.csv", Body=b"id,amount\n1,100\n"
        )

        # Create catalog with S3 sources
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="s3://test-bucket/customers.csv"),
                Dataset(name="orders", source="s3://test-bucket/orders.csv"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch datasets to populate cache
        runner.invoke(app, ["fetch", "customers"])
        runner.invoke(app, ["fetch", "orders"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0
        assert "Total cache size:" in result.output
        assert "Total entries:" in result.output
        assert "Cache directory:" in result.output
        assert "Per-dataset breakdown:" in result.output
        assert "customers:" in result.output
        assert "orders:" in result.output
        # Verify entries count matches
        assert "2" in result.output  # Should show 2 entries

    def test_cache_stats_with_mixed_datasets(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Verify cache-stats correctly shows both cached and missing datasets."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "cached.csv").write_text("id,name\n1,Alice\n")
        (storage_dir / "missing.csv").write_text("id,name\n1,Bob\n")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="cached", source="{storage_dir / "cached.csv"}"),
                Dataset(name="missing", source="{storage_dir / "missing.csv"}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Fetch only one dataset - the other should be missing
        runner.invoke(app, ["fetch", "cached"])

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0
        assert "Total cache size:" in result.output
        assert "Total entries:" in result.output
        assert "Per-dataset breakdown:" in result.output
        # Both datasets should appear in breakdown
        assert "cached:" in result.output
        assert "missing:" in result.output
        # Cached should show size > 0, missing should show 0 B or (missing)
        assert "(fresh)" in result.output or "(stale)" in result.output
        assert "(missing)" in result.output
