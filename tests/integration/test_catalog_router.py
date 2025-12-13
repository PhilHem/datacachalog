"""Integration tests for Catalog with RouterStorage.

These tests verify the Catalog can use RouterStorage to handle
mixed URI schemes (S3 and local filesystem) transparently.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest


if TYPE_CHECKING:
    from pathlib import Path

from datacachalog import Catalog, Dataset
from datacachalog.adapters.cache import FileCache
from datacachalog.adapters.storage import FilesystemStorage, S3Storage
from datacachalog.adapters.storage.router import RouterStorage


@pytest.mark.storage
class TestCatalogRouterMixedSchemes:
    """Tests for Catalog using RouterStorage with mixed URI schemes."""

    def test_fetch_routes_s3_uri_to_s3_storage(self, s3_client, tmp_path: Path) -> None:
        """Catalog with RouterStorage should fetch s3:// URIs via S3Storage."""
        # Setup S3 file
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"s3 content")

        cache_dir = tmp_path / "cache"
        router = RouterStorage(
            backends={
                "s3": S3Storage(client=s3_client),
                None: FilesystemStorage(),
            }
        )
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="s3data", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=router, cache=cache, cache_dir=cache_dir
        )

        path = catalog.fetch("s3data")

        assert path.exists()
        assert path.read_text() == "s3 content"

    def test_fetch_routes_local_path_to_filesystem_storage(
        self, tmp_path: Path
    ) -> None:
        """Catalog with RouterStorage should fetch local paths via FilesystemStorage."""
        # Setup local file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        source_file = source_dir / "local.csv"
        source_file.write_text("local content")

        cache_dir = tmp_path / "cache"
        router = RouterStorage(backends={None: FilesystemStorage()})
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="localdata", source=str(source_file))
        catalog = Catalog(
            datasets=[dataset], storage=router, cache=cache, cache_dir=cache_dir
        )

        path = catalog.fetch("localdata")

        assert path.exists()
        assert path.read_text() == "local content"

    def test_fetch_mixed_datasets_in_single_catalog(
        self, s3_client, tmp_path: Path
    ) -> None:
        """Catalog should handle both S3 and local datasets simultaneously."""
        # Setup S3 file
        s3_client.put_object(Bucket="test-bucket", Key="cloud.csv", Body=b"from s3")

        # Setup local file
        source_dir = tmp_path / "source"
        source_dir.mkdir()
        local_file = source_dir / "disk.csv"
        local_file.write_text("from disk")

        cache_dir = tmp_path / "cache"
        router = RouterStorage(
            backends={
                "s3": S3Storage(client=s3_client),
                None: FilesystemStorage(),
            }
        )
        cache = FileCache(cache_dir=cache_dir)
        datasets = [
            Dataset(name="cloud", source="s3://test-bucket/cloud.csv"),
            Dataset(name="disk", source=str(local_file)),
        ]
        catalog = Catalog(
            datasets=datasets, storage=router, cache=cache, cache_dir=cache_dir
        )

        # Fetch both
        cloud_path = catalog.fetch("cloud")
        disk_path = catalog.fetch("disk")

        assert cloud_path.read_text() == "from s3"
        assert disk_path.read_text() == "from disk"


@pytest.mark.storage
class TestCatalogRouterStaleness:
    """Tests for staleness detection with RouterStorage."""

    def test_is_stale_works_with_router(self, s3_client, tmp_path: Path) -> None:
        """RouterStorage should correctly report staleness from S3."""
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"original")

        cache_dir = tmp_path / "cache"
        router = RouterStorage(
            backends={
                "s3": S3Storage(client=s3_client),
                None: FilesystemStorage(),
            }
        )
        cache = FileCache(cache_dir=cache_dir)
        dataset = Dataset(name="data", source="s3://test-bucket/data.csv")
        catalog = Catalog(
            datasets=[dataset], storage=router, cache=cache, cache_dir=cache_dir
        )

        # Populate cache
        catalog.fetch("data")
        assert catalog.is_stale("data") is False

        # Update S3
        s3_client.put_object(Bucket="test-bucket", Key="data.csv", Body=b"updated")
        assert catalog.is_stale("data") is True
