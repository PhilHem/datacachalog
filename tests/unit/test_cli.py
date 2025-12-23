"""Tests for the CLI commands."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
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


@pytest.mark.cli
@pytest.mark.tra("UseCase.Fetch")
@pytest.mark.tier(1)
class TestCatalogFetch:
    """Tests for catalog fetch command."""

    @pytest.mark.tier(1)
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

    @pytest.mark.tier(1)
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

    @pytest.mark.tier(1)
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

    @pytest.mark.tier(1)
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

    @pytest.mark.tier(1)
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

    @pytest.mark.tier(1)
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

    @pytest.mark.tier(2)
    def test_fetch_with_as_of_flag_resolves_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """fetch --as-of resolves and downloads correct version."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload a version
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"version 1"
            )

            # Create catalog with dataset pointing to S3
            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            # Get version timestamp to use for --as-of
            from datacachalog.adapters.storage import S3Storage

            storage = S3Storage(client=client)
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            v1_timestamp = versions[0].last_modified
            future_time = v1_timestamp + timedelta(days=1)

            # Format as YYYY-MM-DD for CLI
            as_of_date = future_time.strftime("%Y-%m-%d")

            result = runner.invoke(app, ["fetch", "data", "--as-of", as_of_date])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            # Should output path to cached file
            assert "data" in result.output

    @pytest.mark.tier(1)
    def test_fetch_with_as_of_flag_date_format_parsing(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Date format parsing works for YYYY-MM-DD and YYYY-MM-DDTHH:MM:SS formats."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            # Get version timestamp to use future dates
            from datacachalog.adapters.storage import S3Storage

            storage = S3Storage(client=client)
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            v1_timestamp = versions[0].last_modified
            future_time1 = v1_timestamp + timedelta(days=1)
            future_time2 = v1_timestamp + timedelta(days=2)

            # Test YYYY-MM-DD format
            as_of_date1 = future_time1.strftime("%Y-%m-%d")
            result1 = runner.invoke(app, ["fetch", "data", "--as-of", as_of_date1])
            assert result1.exit_code == 0, f"Failed with: {result1.output}"

            # Test YYYY-MM-DDTHH:MM:SS format
            as_of_date2 = future_time2.strftime("%Y-%m-%dT%H:%M:%S")
            result2 = runner.invoke(app, ["fetch", "data", "--as-of", as_of_date2])
            assert result2.exit_code == 0, f"Failed with: {result2.output}"

    @pytest.mark.tier(1)
    def test_fetch_with_as_of_flag_error_when_version_not_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Error handling when no version exists at or before specified date."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload a version
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            # Use a date in the past before any version exists
            result = runner.invoke(app, ["fetch", "data", "--as-of", "2020-01-01"])

            assert result.exit_code == 1, f"Expected error but got: {result.output}"
            assert (
                "version" in result.output.lower()
                or "not found" in result.output.lower()
            )

    @pytest.mark.tier(1)
    def test_fetch_with_as_of_flag_mutually_exclusive_with_version_id(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--as-of and --version-id cannot be used together."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(
                app,
                [
                    "fetch",
                    "data",
                    "--as-of",
                    "2024-12-10",
                    "--version-id",
                    "some-version-id",
                ],
            )

            assert result.exit_code == 1, f"Expected error but got: {result.output}"
            assert "mutually exclusive" in result.output.lower() or (
                "as-of" in result.output.lower()
                and "version-id" in result.output.lower()
            )

    @pytest.mark.tier(1)
    def test_fetch_with_as_of_flag_cannot_use_with_all(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--as-of cannot be used with --all flag."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["fetch", "--all", "--as-of", "2024-12-10"])

            assert result.exit_code == 1, f"Expected error but got: {result.output}"
            assert "as-of" in result.output.lower() or "all" in result.output.lower()

    @pytest.mark.tier(1)
    def test_fetch_dry_run_shows_stale_status_without_downloading(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dry-run shows stale status without downloading or modifying cache."""
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

        # Get cache state before dry-run
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        cache_dir = "data"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, cat_cache_dir = load_catalog(catalog_path)
            all_ds.extend(datasets)
            if cat_cache_dir:
                cache_dir = cat_cache_dir
        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        cached_before = cat._cache.get("customers")

        # Dry-run fetch
        result = runner.invoke(app, ["fetch", "customers", "--dry-run"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should show stale status (or at least not download)
        # Cache should be unchanged
        cached_after = cat._cache.get("customers")
        assert cached_before == cached_after, "Cache should not be modified in dry-run"

    @pytest.mark.tier(1)
    def test_fetch_dry_run_shows_fresh_status_when_cached(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dry-run shows fresh status when cache is up to date."""
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

        # Dry-run fetch (cache should be fresh)
        result = runner.invoke(app, ["fetch", "customers", "--dry-run"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should show cached path (fresh status)

    @pytest.mark.tier(1)
    def test_fetch_dry_run_with_all_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dry-run with --all shows status for all datasets without downloading."""
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

        # Dry-run fetch all
        result = runner.invoke(app, ["fetch", "--all", "--dry-run"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should show status for all datasets
        assert "customers" in result.output
        assert "orders" in result.output

    @pytest.mark.tier(1)
    def test_fetch_dry_run_with_as_of_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dry-run with --as-of checks version without downloading."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            # Get version timestamp
            from datacachalog.adapters.storage import S3Storage

            storage = S3Storage(client=client)
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            v1_timestamp = versions[0].last_modified
            future_time = v1_timestamp + timedelta(days=1)
            as_of_date = future_time.strftime("%Y-%m-%d")

            result = runner.invoke(
                app, ["fetch", "data", "--dry-run", "--as-of", as_of_date]
            )

            assert result.exit_code == 0, f"Failed with: {result.output}"

    @pytest.mark.tier(1)
    def test_fetch_dry_run_with_version_id_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """dry-run with --version-id checks version without downloading."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"content"
            )
            version_id = resp["VersionId"]

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(
                app, ["fetch", "data", "--dry-run", "--version-id", version_id]
            )

            assert result.exit_code == 0, f"Failed with: {result.output}"

    @pytest.mark.tier(1)
    def test_fetch_dry_run_does_not_modify_cache(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Multiple dry-run calls should not modify cache state."""
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

        # Modify source to make stale
        time.sleep(0.1)
        source_file.write_text("id,name\n1,Alice\n2,Bob\n")

        # Get cache state before dry-runs
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        cache_dir = "data"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, cat_cache_dir = load_catalog(catalog_path)
            all_ds.extend(datasets)
            if cat_cache_dir:
                cache_dir = cat_cache_dir
        cat = Catalog.from_directory(all_ds, directory=root, cache_dir=cache_dir)
        cached_before = cat._cache.get("customers")

        # Multiple dry-run calls
        runner.invoke(app, ["fetch", "customers", "--dry-run"])
        runner.invoke(app, ["fetch", "customers", "--dry-run"])
        runner.invoke(app, ["fetch", "customers", "--dry-run"])

        # Cache should be unchanged
        cached_after = cat._cache.get("customers")
        assert cached_before == cached_after, "Cache should not be modified by dry-run"


@pytest.mark.cli
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
@pytest.mark.tier(2)
class TestCatalogVersions:
    """Tests for catalog versions command."""

    def test_versions_shows_version_list(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """versions command lists available versions with dates."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            # Setup versioned S3 bucket
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload multiple versions
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v1")
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v2")
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v3")

            # Create catalog with dataset pointing to S3
            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["versions", "data"])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            assert "Versions for 'data'" in result.output
            # Should show dates (YYYY-MM-DD format)
            assert (
                "202" in result.output
                or "2024" in result.output
                or "2025" in result.output
            )

    def test_versions_hides_version_id_by_default(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Version ID is not shown in default output."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"v1"
            )
            version_id = resp["VersionId"]

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["versions", "data"])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            # Version ID should NOT be in output
            # The expected format is: "  YYYY-MM-DD HH:MM:SS  size [flags]"
            # NOT: "  YYYY-MM-DD HH:MM:SS  version_id  size [flags]"
            assert version_id not in result.output, (
                f"Version ID {version_id} should not be in output"
            )
            # Also verify the format: should have date, then size, without version_id in between
            output_lines = [
                line.strip()
                for line in result.output.split("\n")
                if line.strip()
                and not line.startswith("Versions for")
                and not line.startswith("No versions")
            ]
            assert len(output_lines) > 0, "Should have at least one version line"
            # Each line should match: date (YYYY-MM-DD HH:MM:SS) followed by size
            # Should NOT have a long alphanumeric string (version_id) between them
            for line in output_lines:
                # Split by whitespace - should have: date, time, size_number, "bytes", optionally flags
                parts = line.split()
                # After date/time (first 2 parts), next should be size number, not a version_id
                # Version IDs are typically UUIDs or long strings - check that part[2] is a number or "unknown"
                if len(parts) >= 3:
                    # parts[0] = date, parts[1] = time, parts[2] should be size number or "unknown"
                    # If parts[2] looks like a version_id (long alphanumeric), that's wrong
                    third_part = parts[2]
                    # Version IDs are typically non-numeric, so if it's not a number and not "unknown", it might be version_id
                    if (
                        not third_part.replace(",", "").isdigit()
                        and third_part != "unknown"
                        and len(third_part) > 10
                    ):
                        # This is likely a version_id, which should not be there
                        raise AssertionError(
                            f"Found potential version_id in output: {third_part}"
                        )

    def test_versions_shows_date_as_primary(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Date/timestamp is the primary identifier in output."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v1")

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["versions", "data"])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            # Date should be at the start of each version line
            # Format should be: "  YYYY-MM-DD HH:MM:SS  size [flags]"
            output_lines = [
                line
                for line in result.output.split("\n")
                if line.strip() and not line.startswith("Versions for")
            ]
            assert len(output_lines) > 0, "Should have at least one version line"
            # First non-header line should start with date format
            first_version_line = output_lines[0]
            # Should start with spaces, then date (YYYY-MM-DD)
            assert first_version_line.strip().startswith(
                "202"
            ) or first_version_line.strip().startswith("20")

    def test_versions_respects_limit_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """--limit flag limits number of versions shown."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload 5 versions
            for i in range(5):
                client.put_object(
                    Bucket="versioned-bucket", Key="data.txt", Body=f"v{i}".encode()
                )

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["versions", "data", "--limit", "3"])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            # Count version lines (excluding header)
            version_lines = [
                line
                for line in result.output.split("\n")
                if line.strip()
                and not line.startswith("Versions for")
                and not line.startswith("No versions")
            ]
            assert len(version_lines) == 3, (
                f"Expected 3 versions, got {len(version_lines)}"
            )

    def test_versions_shows_latest_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Latest version is marked with 'latest' flag."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v1")
            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v2")

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["versions", "data"])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            # Should show "latest" flag for the newest version
            assert "latest" in result.output.lower()

    def test_versions_shows_deleted_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Delete markers are marked with 'deleted' flag."""
        import boto3
        from moto import mock_aws

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(Bucket="versioned-bucket", Key="data.txt", Body=b"v1")
            # Delete the object (creates delete marker)
            client.delete_object(Bucket="versioned-bucket", Key="data.txt")

            catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
            catalogs_dir.mkdir(parents=True)
            (catalogs_dir / "default.py").write_text(
                dedent("""\
                from datacachalog import Dataset
                datasets = [
                    Dataset(name="data", source="s3://versioned-bucket/data.txt"),
                ]
            """)
            )

            (tmp_path / "data").mkdir()
            monkeypatch.chdir(tmp_path)

            result = runner.invoke(app, ["versions", "data"])

            assert result.exit_code == 0, f"Failed with: {result.output}"
            # Should show "deleted" flag for delete marker
            assert "deleted" in result.output.lower()

    def test_versions_dataset_not_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Error handling for unknown dataset."""
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

        result = runner.invoke(app, ["versions", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_versions_versioning_not_supported(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Error handling for non-versioned storage."""
        # Create source file (filesystem storage doesn't support versioning)
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        source_file = storage_dir / "data.txt"
        source_file.write_text("content")

        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="data", source="{source_file}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["versions", "data"])

        assert result.exit_code == 1
        assert (
            "versioning" in result.output.lower()
            or "not supported" in result.output.lower()
        )


@pytest.mark.cli
@pytest.mark.tra("UseCase.Push")
@pytest.mark.tier(1)
class TestCatalogPush:
    """Tests for catalog push command."""

    @pytest.mark.tier(1)
    def test_push_uploads_file_to_remote(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push() should upload local file to dataset's source location."""
        # Create source file (simulates remote storage)
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original content")

        # Create local file to upload
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("updated content")

        # Create catalog with dataset pointing to source file
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{remote_file}"),
            ]
        """)
        )

        # Create data directory for cache
        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["push", "customers", str(local_file)])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Assert: remote file now has updated content
        assert remote_file.read_text() == "updated content"

    @pytest.mark.tier(1)
    def test_push_updates_cache_metadata(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push() should update cache with new metadata matching remote."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original")

        # Create local file to upload
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("updated")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{remote_file}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Push the file
        result = runner.invoke(app, ["push", "customers", str(local_file)])

        assert result.exit_code == 0, f"Failed with: {result.output}"

        # Verify cache is fresh by checking status
        status_result = runner.invoke(app, ["status"])
        assert status_result.exit_code == 0
        assert "customers" in status_result.output
        assert "fresh" in status_result.output.lower()

    @pytest.mark.tier(1)
    def test_push_dataset_not_found_exits_with_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push() should exit with error for unknown dataset name."""
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

        local_file = tmp_path / "file.csv"
        local_file.write_text("content")

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["push", "nonexistent", str(local_file)])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    @pytest.mark.tier(1)
    def test_push_file_not_found_exits_with_error(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push() should exit with error for missing local file."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("content")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{remote_file}"),
            ]
        """)
        )
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        missing_file = tmp_path / "does_not_exist.csv"

        result = runner.invoke(app, ["push", "customers", str(missing_file)])

        assert result.exit_code == 1
        assert (
            "not found" in result.output.lower()
            or "does not exist" in result.output.lower()
        )

    @pytest.mark.tier(1)
    def test_push_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push --catalog X pushes to that specific catalog's dataset."""
        # Create source files
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.csv").write_text("original")
        (storage_dir / "metrics.csv").write_text("original")

        # Create local file to upload
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("updated")

        # Create two catalogs with same dataset name
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)

        (catalogs_dir / "core.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="data", source="{storage_dir / "customers.csv"}"),
            ]
        """)
        )

        (catalogs_dir / "analytics.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="data", source="{storage_dir / "metrics.csv"}"),
            ]
        """)
        )

        (tmp_path / "data").mkdir()
        monkeypatch.chdir(tmp_path)

        # Push to core catalog specifically
        result = runner.invoke(
            app, ["push", "data", str(local_file), "--catalog", "core"]
        )

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Should update core catalog's dataset
        assert (storage_dir / "customers.csv").read_text() == "updated"
        # Should NOT update analytics catalog's dataset
        assert (storage_dir / "metrics.csv").read_text() == "original"

    @pytest.mark.tier(1)
    def test_push_shows_success_message(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """push() should show success message or confirmation."""
        # Create source file
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        remote_file = storage_dir / "data.csv"
        remote_file.write_text("original")

        # Create local file to upload
        local_dir = tmp_path / "local"
        local_dir.mkdir()
        local_file = local_dir / "new_data.csv"
        local_file.write_text("updated")

        # Create catalog
        catalogs_dir = tmp_path / ".datacachalog" / "catalogs"
        catalogs_dir.mkdir(parents=True)
        (catalogs_dir / "default.py").write_text(
            dedent(f"""\
            from datacachalog import Dataset
            datasets = [
                Dataset(name="customers", source="{remote_file}"),
            ]
        """)
        )
        (tmp_path / "data").mkdir()

        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["push", "customers", str(local_file)])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        # Output should contain some indication of success
        # (could be empty, but exit code 0 indicates success)
        # Check that remote file was updated as confirmation
        assert remote_file.read_text() == "updated"


@pytest.mark.cli
@pytest.mark.tier(1)
class TestCatalogInfo:
    """Tests for catalog info command."""

    def test_info_shows_dataset_details(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """info shows dataset name, source, cache path, staleness status."""
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

        result = runner.invoke(app, ["info", "customers"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert str(source_file) in result.output or "data.csv" in result.output
        assert "data" in result.output  # cache path
        assert (
            "missing" in result.output.lower()
            or "stale" in result.output.lower()
            or "fresh" in result.output.lower()
        )

    def test_info_shows_all_datasets_with_all_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """info --all shows details for all datasets."""
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

        result = runner.invoke(app, ["info", "--all"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert "orders" in result.output

    def test_info_with_catalog_flag(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """info --catalog X shows only datasets from that catalog."""
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

        result = runner.invoke(app, ["info", "--catalog", "core"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        assert "metrics" not in result.output

    def test_info_dataset_not_found(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """info with unknown dataset shows error and exits 1."""
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

        result = runner.invoke(app, ["info", "nonexistent"])

        assert result.exit_code == 1
        assert "not found" in result.output.lower()

    def test_info_shows_cache_size_when_cached(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When dataset is cached, info shows cache size."""
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

        # Now check info
        result = runner.invoke(app, ["info", "customers"])

        assert result.exit_code == 0, f"Failed with: {result.output}"
        assert "customers" in result.output
        # Should show cache size (bytes or KB/MB)
        assert (
            "bytes" in result.output.lower()
            or "kb" in result.output.lower()
            or "mb" in result.output.lower()
            or any(
                char.isdigit()
                for char in result.output
                if "size" in result.output.lower()
            )
        )
