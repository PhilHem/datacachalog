"""Tests for CLI catalog_root parameter validation."""

from pathlib import Path
from textwrap import dedent
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.CatalogRoot")
@pytest.mark.tier(1)
class TestLoadCatalogContext:
    """Tests for load_catalog_context catalog_root parameter."""

    def test_load_catalog_context_passes_catalog_root(self, tmp_path: Path) -> None:
        """load_catalog_context passes catalog_root to load_catalog."""
        from datacachalog.cli.main import load_catalog_context

        # Create a valid catalog structure
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="s3://bucket/test.parquet"),
            ]
            cache_dir = "data"
        """)
        )
        (tmp_path / ".git").mkdir()

        with patch("datacachalog.config.find_project_root", return_value=tmp_path):
            catalog, root, _catalogs = load_catalog_context()

            # Verify the catalog loaded successfully
            assert catalog is not None
            assert len(catalog.datasets) > 0
            assert root == tmp_path

    def test_load_catalog_context_rejects_path_traversal(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """load_catalog_context rejects catalogs outside .datacachalog/catalogs/."""
        import typer

        from datacachalog.cli.main import load_catalog_context

        # Create the valid catalog directory
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        # Create a malicious catalog outside the allowed directory
        evil_dir = tmp_path / "evil"
        evil_dir.mkdir()
        (evil_dir / "malicious.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="evil", source="s3://bucket/evil.parquet"),
            ]
        """)
        )

        # Create a default catalog
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

        evil_catalog_path = evil_dir / "malicious.py"

        with (
            patch(
                "datacachalog.discovery.discover_catalogs",
                return_value={"evil": evil_catalog_path},
            ),
            pytest.raises(typer.Exit),
        ):
            # Should raise UnsafeCatalogPathError when load_catalog_context tries
            # to load the malicious catalog
            load_catalog_context("evil")


@pytest.mark.cli
@pytest.mark.tra("UseCase.Info")
@pytest.mark.tier(1)
class TestInfoCommandCatalogRoot:
    """Tests for info command catalog_root parameter."""

    def test_info_command_passes_catalog_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """info command passes catalog_root to load_catalog."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="s3://bucket/test.parquet"),
            ]
            cache_dir = "data"
        """)
        )
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["info", "--all"])

        assert result.exit_code == 0, f"Failed with: {result.output}"


@pytest.mark.cli
@pytest.mark.tra("UseCase.CacheStats")
@pytest.mark.tier(1)
class TestCacheStatsCommandCatalogRoot:
    """Tests for cache-stats command catalog_root parameter."""

    def test_cache_stats_command_passes_catalog_root(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """cache-stats command passes catalog_root to load_catalog."""
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent("""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="test", source="s3://bucket/test.parquet"),
            ]
            cache_dir = "data"
        """)
        )
        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["cache-stats"])

        assert result.exit_code == 0


@pytest.mark.cli
@pytest.mark.tra("UseCase.CatalogRoot")
@pytest.mark.tier(1)
class TestCliEnforcesCatalogRootValidation:
    """Tests for CLI enforcing catalog_root validation."""

    def test_cli_enforces_catalog_root_validation(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """CLI rejects catalogs loaded from outside .datacachalog/catalogs/."""
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

        # Create an evil catalog outside the allowed directory
        evil_dir = tmp_path / "evil"
        evil_dir.mkdir()
        (evil_dir / "bad.py").write_text(
            dedent("""\
            import os
            os.system("echo 'SECURITY_BREACH'")
            datasets = []
        """)
        )

        (tmp_path / ".git").mkdir()
        monkeypatch.chdir(tmp_path)

        evil_path = evil_dir / "bad.py"

        with patch(
            "datacachalog.discovery.discover_catalogs",
            return_value={"bad": evil_path},
        ):
            # Run the list command, which should fail when trying to load the
            # malicious catalog due to catalog_root validation
            result = runner.invoke(app, ["list"])

            # Should exit with error code, not execute the malicious code
            assert result.exit_code != 0
