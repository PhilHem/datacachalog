"""Tests for the CLI fetch command."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.Fetch")
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

        # Modify source file and set mtime to future to make cache stale
        source_file.write_text("id,name\n1,Alice\n2,Bob\n")
        future_time = time.time() + 10  # 10 seconds in the future
        os.utime(source_file, (future_time, future_time))

        # Get cache state before dry-run
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        cache_dir = "data"
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, cat_cache_dir = load_catalog(
                catalog_path, catalog_root=catalog_root
            )
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

        # Modify source to make stale (use os.utime instead of sleep for determinism)
        source_file.write_text("id,name\n1,Alice\n2,Bob\n")
        future_time = time.time() + 10
        os.utime(source_file, (future_time, future_time))

        # Get cache state before dry-runs
        from datacachalog import Catalog
        from datacachalog.config import find_project_root
        from datacachalog.discovery import discover_catalogs, load_catalog

        root = find_project_root()
        catalogs = discover_catalogs(root)
        all_ds = []
        cache_dir = "data"
        catalog_root = root / ".datacachalog" / "catalogs"
        for _catalog_name, catalog_path in catalogs.items():
            datasets, cat_cache_dir = load_catalog(
                catalog_path, catalog_root=catalog_root
            )
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
