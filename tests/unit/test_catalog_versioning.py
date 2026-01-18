"""Unit tests for Catalog versioning operations."""

from pathlib import Path

import pytest

from datacachalog import Dataset


@pytest.mark.core
@pytest.mark.tra("UseCase.Versions")
@pytest.mark.tier(1)
class TestVersions:
    """Tests for catalog.versions() method."""

    @pytest.mark.tier(2)
    def test_versions_returns_object_versions(self, tmp_path: Path) -> None:
        """versions() should return list of ObjectVersion for dataset."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.models import ObjectVersion
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Act
            versions = catalog.versions("data")

            # Assert
            assert isinstance(versions, list)
            assert len(versions) == 3
            assert all(isinstance(v, ObjectVersion) for v in versions)

    @pytest.mark.tier(2)
    def test_versions_respects_limit(self, tmp_path: Path) -> None:
        """versions(limit=N) should return at most N versions."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Act
            versions = catalog.versions("data", limit=3)

            # Assert
            assert len(versions) == 3

    @pytest.mark.tier(1)
    def test_versions_raises_dataset_not_found(self, tmp_path: Path) -> None:
        """versions() should raise DatasetNotFoundError for unknown dataset."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import DatasetNotFoundError
        from datacachalog.core.services import Catalog

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        catalog = Catalog(datasets=[], storage=storage, cache=cache)

        with pytest.raises(DatasetNotFoundError, match="unknown"):
            catalog.versions("unknown")

    @pytest.mark.tier(1)
    def test_versions_raises_on_non_versioned_storage(self, tmp_path: Path) -> None:
        """versions() should raise VersioningNotSupportedError for filesystem."""
        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.exceptions import VersioningNotSupportedError
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.txt").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "data.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(VersioningNotSupportedError):
            catalog.versions("data")


@pytest.mark.core
@pytest.mark.tra("UseCase.FetchVersion")
@pytest.mark.tier(2)
class TestFetchVersion:
    """Tests for fetch() with version_id parameter."""

    def test_fetch_with_version_id_downloads_specific_version(
        self, tmp_path: Path
    ) -> None:
        """fetch(version_id=) should download that specific version."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Upload two versions
            resp1 = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"first version"
            )
            v1_id = resp1["VersionId"]
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"second version"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch the first version (not the latest)
            result = catalog.fetch("data", version_id=v1_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            assert path.exists()
            assert path.read_text() == "first version"

    def test_fetch_version_uses_version_aware_cache_key(self, tmp_path: Path) -> None:
        """Versioned fetches should cache under version-specific key."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch with version_id
            result = catalog.fetch("data", version_id=version_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # Cache key should be date-based (not {name}@{version_id})
            filename = path.name
            assert filename.endswith(".txt")
            # Should be date-based format: YYYY-MM-DDTHHMMSS.txt
            date_part = filename[:-4]
            assert len(date_part) == 17  # YYYY-MM-DDTHHMMSS
            assert date_part[10] == "T"  # Date-time separator
            # Verify it's cached
            assert cache.get(filename) is not None

    def test_fetch_version_caches_separately_from_latest(self, tmp_path: Path) -> None:
        """Versioned fetch and normal fetch should use separate cache entries."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            resp1 = client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"old version"
            )
            v1_id = resp1["VersionId"]
            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"new version"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch latest (normal)
            result_latest = catalog.fetch("data")
            assert isinstance(result_latest, Path)  # Type narrowing
            latest_path = result_latest

            # Fetch specific old version
            result_old = catalog.fetch("data", version_id=v1_id)
            assert isinstance(result_old, Path)  # Type narrowing
            old_path = result_old

            # Both should exist with different content
            assert latest_path.read_text() == "new version"
            assert old_path.read_text() == "old version"

            # Should be different cache entries
            assert cache.get("data") is not None  # latest uses dataset name
            # Versioned uses date-based key (filename from path)
            assert (
                cache.get(old_path.name) is not None
            )  # versioned uses date-based filename

    def test_fetch_version_uses_date_based_file_path(self, tmp_path: Path) -> None:
        """Versioned fetches should use date-based file paths (YYYY-MM-DDTHHMMSS.ext)."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch with version_id
            result = catalog.fetch("data", version_id=version_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # File path should be date-based format: YYYY-MM-DDTHHMMSS.ext
            filename = path.name
            assert filename.endswith(".txt")
            # Check format: YYYY-MM-DDTHHMMSS.txt (no colons in time part)
            date_part = filename[:-4]  # Remove .txt extension
            assert (
                len(date_part) == 17
            )  # YYYY-MM-DDTHHMMSS = 17 chars (4+1+2+1+2+1+2+2+2)
            assert date_part[4] == "-"  # Year-month separator
            assert date_part[7] == "-"  # Month-day separator
            assert date_part[10] == "T"  # Date-time separator
            # Time part should have no colons (HHMMSS format)
            assert ":" not in date_part

    def test_fetch_version_date_format_matches_version_timestamp(
        self, tmp_path: Path
    ) -> None:
        """The date in filename should match the version's last_modified timestamp."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            # Get version metadata to check timestamp
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            version_meta = next(v for v in versions if v.version_id == version_id)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Fetch with version_id
            result = catalog.fetch("data", version_id=version_id)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # Extract date from filename and compare with version timestamp
            filename = path.name
            date_part = filename[:-4]  # Remove .txt extension
            # Parse date components from format YYYY-MM-DDTHHMMSS
            year = int(date_part[0:4])
            month = int(date_part[5:7])
            day = int(date_part[8:10])
            hour = int(date_part[11:13])
            minute = int(date_part[13:15])
            second = int(date_part[15:17])

            from datetime import UTC

            expected_dt = version_meta.last_modified.replace(tzinfo=UTC)
            assert year == expected_dt.year
            assert month == expected_dt.month
            assert day == expected_dt.day
            assert hour == expected_dt.hour
            assert minute == expected_dt.minute
            assert second == expected_dt.second

    def test_fetch_version_preserves_file_extension(self, tmp_path: Path) -> None:
        """The file extension from original source should be preserved."""
        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            # Test with different extensions
            for ext in [".txt", ".parquet", ".csv", ".json"]:
                key = f"data{ext}"
                resp = client.put_object(
                    Bucket="versioned-bucket", Key=key, Body=b"content"
                )
                version_id = resp["VersionId"]

                cache_dir = tmp_path / "cache"
                storage = S3Storage(client=client)
                cache = FileCache(cache_dir=cache_dir)

                dataset = Dataset(name="data", source=f"s3://versioned-bucket/{key}")
                catalog = Catalog(
                    datasets=[dataset],
                    storage=storage,
                    cache=cache,
                    cache_dir=cache_dir,
                )

                # Fetch with version_id
                result = catalog.fetch("data", version_id=version_id)
                assert isinstance(result, Path)  # Type narrowing
                path = result

                # Extension should be preserved
                assert path.suffix == ext
                assert path.name.endswith(ext)


@pytest.mark.core
@pytest.mark.tra("UseCase.FetchAsOf")
@pytest.mark.tier(1)
class TestFetchAsOf:
    """Tests for fetch() with as_of parameter."""

    @pytest.mark.tier(2)
    def test_fetch_with_as_of_resolves_correct_version(self, tmp_path: Path) -> None:
        """fetch(as_of=datetime) should download version at that time."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Get the version timestamp
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            v1_timestamp = versions[0].last_modified

            # Use a time in the future (should get the only version)
            future_time = v1_timestamp + timedelta(days=1)
            result = catalog.fetch("data", as_of=future_time)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            assert path.exists()
            assert path.read_text() == "version 1"

    @pytest.mark.tier(2)
    def test_fetch_with_as_of_uses_version_id_resolution(self, tmp_path: Path) -> None:
        """as_of should resolve to version_id and use _fetch_version."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.services import Catalog

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

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            as_of = versions[0].last_modified + timedelta(seconds=1)

            # Fetch with as_of
            result = catalog.fetch("data", as_of=as_of)
            assert isinstance(result, Path)  # Type narrowing
            path = result

            # Should have cached with date-based key (not {name}@{version_id})
            filename = path.name
            assert filename.endswith(".txt")
            # Should be date-based format: YYYY-MM-DDTHHMMSS.txt
            assert cache.get(filename) is not None

    @pytest.mark.tier(1)
    def test_fetch_as_of_and_version_id_mutually_exclusive(
        self, tmp_path: Path
    ) -> None:
        """fetch() should raise ValueError if both as_of and version_id given."""
        from datetime import datetime

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "data.txt").write_text("content")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        dataset = Dataset(name="data", source=str(storage_dir / "data.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(ValueError, match="mutually exclusive"):
            catalog.fetch("data", as_of=datetime.now(), version_id="abc123")

    @pytest.mark.tier(1)
    def test_fetch_version_on_glob_raises_error(self, tmp_path: Path) -> None:
        """Versioned fetch on glob dataset should raise clear error."""
        from datetime import datetime

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import FilesystemStorage
        from datacachalog.core.services import Catalog

        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "a.txt").write_text("a")
        (storage_dir / "b.txt").write_text("b")

        cache_dir = tmp_path / "cache"
        storage = FilesystemStorage()
        cache = FileCache(cache_dir=cache_dir)

        # Glob dataset
        dataset = Dataset(name="data", source=str(storage_dir / "*.txt"))
        catalog = Catalog(
            datasets=[dataset],
            storage=storage,
            cache=cache,
            cache_dir=cache_dir,
        )

        with pytest.raises(ValueError, match="glob"):
            catalog.fetch("data", version_id="abc123")

        with pytest.raises(ValueError, match="glob"):
            catalog.fetch("data", as_of=datetime.now())

    @pytest.mark.tier(2)
    def test_fetch_as_of_raises_version_not_found_if_no_match(
        self, tmp_path: Path
    ) -> None:
        """as_of before any version should raise VersionNotFoundError."""
        from datetime import timedelta

        import boto3
        from moto import mock_aws

        from datacachalog.adapters.cache import FileCache
        from datacachalog.adapters.storage import S3Storage
        from datacachalog.core.exceptions import VersionNotFoundError
        from datacachalog.core.services import Catalog

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="versioned-bucket")
            client.put_bucket_versioning(
                Bucket="versioned-bucket",
                VersioningConfiguration={"Status": "Enabled"},
            )

            client.put_object(
                Bucket="versioned-bucket", Key="data.txt", Body=b"version 1"
            )

            cache_dir = tmp_path / "cache"
            storage = S3Storage(client=client)
            cache = FileCache(cache_dir=cache_dir)

            dataset = Dataset(name="data", source="s3://versioned-bucket/data.txt")
            catalog = Catalog(
                datasets=[dataset],
                storage=storage,
                cache=cache,
                cache_dir=cache_dir,
            )

            # Get version timestamp and use a time before it
            versions = storage.list_versions("s3://versioned-bucket/data.txt")
            before_all = versions[0].last_modified - timedelta(days=365)

            with pytest.raises(VersionNotFoundError) as exc_info:
                catalog.fetch("data", as_of=before_all)

            assert exc_info.value.name == "data"
            assert exc_info.value.recovery_hint is not None
