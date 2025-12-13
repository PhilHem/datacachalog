"""Tests validating that example code patterns work correctly.

These tests ensure the examples in the examples/ directory represent
working, copy-pasteable code patterns.
"""

from pathlib import Path

import pytest

from datacachalog import Catalog, Dataset, FileCache, FilesystemStorage


@pytest.mark.core
class TestBasicUsage:
    """Tests for basic_usage.py example pattern."""

    def test_single_dataset_fetch(self, tmp_path: Path) -> None:
        """Single dataset can be fetched and cached."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "customers.parquet").write_bytes(b"mock parquet data")

        dataset = Dataset(
            name="customers",
            source=str(storage_dir / "customers.parquet"),
            description="Customer master data",
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=FilesystemStorage(),
            cache=FileCache(tmp_path / "cache"),
            cache_dir=tmp_path / "cache",
        )

        path = catalog.fetch("customers")

        assert path.exists()
        assert path.read_bytes() == b"mock parquet data"


@pytest.mark.core
class TestParallelFetch:
    """Tests for parallel_fetch.py example pattern."""

    def test_fetch_all_downloads_multiple_datasets(self, tmp_path: Path) -> None:
        """fetch_all() downloads all datasets and returns paths."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        (storage_dir / "orders.csv").write_text("order_id,amount\n1,100")
        (storage_dir / "products.csv").write_text("product_id,name\n1,Widget")
        (storage_dir / "customers.csv").write_text("id,name\n1,Alice")

        datasets = [
            Dataset(name="orders", source=str(storage_dir / "orders.csv")),
            Dataset(name="products", source=str(storage_dir / "products.csv")),
            Dataset(name="customers", source=str(storage_dir / "customers.csv")),
        ]
        catalog = Catalog(
            datasets=datasets,
            storage=FilesystemStorage(),
            cache=FileCache(tmp_path / "cache"),
            cache_dir=tmp_path / "cache",
        )

        paths = catalog.fetch_all()

        assert len(paths) == 3
        assert all(p.exists() for p in paths.values())
        assert paths["orders"].read_text() == "order_id,amount\n1,100"
        assert paths["products"].read_text() == "product_id,name\n1,Widget"
        assert paths["customers"].read_text() == "id,name\n1,Alice"


@pytest.mark.core
class TestPushWorkflow:
    """Tests for push_workflow.py example pattern."""

    def test_push_uploads_processed_file(self, tmp_path: Path) -> None:
        """push() uploads local file to remote storage."""
        storage_dir = tmp_path / "storage"
        storage_dir.mkdir()
        original = storage_dir / "report.csv"
        original.write_text("old data")

        dataset = Dataset(name="report", source=str(original))
        catalog = Catalog(
            datasets=[dataset],
            storage=FilesystemStorage(),
            cache=FileCache(tmp_path / "cache"),
            cache_dir=tmp_path / "cache",
        )

        # Simulate processing: create new local file
        processed = tmp_path / "processed_report.csv"
        processed.write_text("new processed data")

        catalog.push("report", local_path=processed)

        assert original.read_text() == "new processed data"


@pytest.mark.core
class TestErrorHandling:
    """Tests for error_handling.py example pattern."""

    def test_dataset_not_found_error_with_recovery_hint(self, tmp_path: Path) -> None:
        """DatasetNotFoundError includes recovery hint with available names."""
        from datacachalog import DatasetNotFoundError

        catalog = Catalog(
            datasets=[Dataset(name="exists", source="/any/path.csv")],
            storage=FilesystemStorage(),
            cache=FileCache(tmp_path / "cache"),
            cache_dir=tmp_path / "cache",
        )

        with pytest.raises(DatasetNotFoundError) as exc:
            catalog.fetch("unknown")

        assert "exists" in exc.value.recovery_hint

    def test_storage_not_found_error(self, tmp_path: Path) -> None:
        """StorageNotFoundError raised when remote file missing."""
        from datacachalog import StorageNotFoundError

        catalog = Catalog(
            datasets=[Dataset(name="missing", source="/nonexistent/file.csv")],
            storage=FilesystemStorage(),
            cache=FileCache(tmp_path / "cache"),
            cache_dir=tmp_path / "cache",
        )

        with pytest.raises(StorageNotFoundError) as exc:
            catalog.fetch("missing")

        assert "nonexistent" in str(exc.value)


@pytest.mark.core
class TestLocalDevelopment:
    """Tests for local_development.py example pattern."""

    def test_filesystem_storage_for_local_testing(self, tmp_path: Path) -> None:
        """FilesystemStorage enables testing without S3."""
        mock_s3 = tmp_path / "mock_s3" / "bucket" / "data"
        mock_s3.mkdir(parents=True)
        (mock_s3 / "users.json").write_text('{"users": []}')

        dataset = Dataset(
            name="users",
            source=str(mock_s3 / "users.json"),
        )
        catalog = Catalog(
            datasets=[dataset],
            storage=FilesystemStorage(),
            cache=FileCache(tmp_path / "cache"),
            cache_dir=tmp_path / "cache",
        )

        path = catalog.fetch("users")

        assert path.read_text() == '{"users": []}'
