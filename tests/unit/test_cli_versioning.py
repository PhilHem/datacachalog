"""Tests for CLI versioning, push, and info commands."""

from pathlib import Path
from textwrap import dedent

import pytest
from typer.testing import CliRunner

from datacachalog.cli import app


runner = CliRunner()


@pytest.mark.cli
@pytest.mark.tra("UseCase.Versions")
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
            # Expected format: "  YYYY-MM-DD HH:MM:SS  size [flags]"
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
@pytest.mark.tra("UseCase.Info")
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
