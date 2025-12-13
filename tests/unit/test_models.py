"""Unit tests for core domain models.

These tests verify the behavior of Dataset, FileMetadata, and CacheMetadata.
They are pure unit tests with no I/O dependencies.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from datacachalog.core.models import CacheMetadata, Dataset, FileMetadata


class TestDataset:
    """Tests for the Dataset model."""

    @pytest.mark.core
    def test_dataset_creation_minimal(self) -> None:
        """Dataset can be created with just name and source."""
        dataset = Dataset(name="test", source="s3://bucket/file.parquet")

        assert dataset.name == "test"
        assert dataset.source == "s3://bucket/file.parquet"
        assert dataset.description == ""
        assert dataset.cache_path is None

    @pytest.mark.core
    def test_dataset_creation_full(self) -> None:
        """Dataset can be created with all fields."""
        cache_path = Path("/tmp/data/file.parquet")
        dataset = Dataset(
            name="customers",
            source="s3://bucket/customers/data.parquet",
            description="Customer master data",
            cache_path=cache_path,
        )

        assert dataset.name == "customers"
        assert dataset.source == "s3://bucket/customers/data.parquet"
        assert dataset.description == "Customer master data"
        assert dataset.cache_path == cache_path

    @pytest.mark.core
    def test_dataset_immutable(self) -> None:
        """Dataset is immutable (frozen dataclass)."""
        dataset = Dataset(name="test", source="s3://bucket/file.parquet")

        with pytest.raises(AttributeError):
            dataset.name = "new_name"  # type: ignore[misc]

    @pytest.mark.core
    def test_dataset_empty_name_raises(self) -> None:
        """Dataset raises ValueError for empty name."""
        with pytest.raises(ValueError, match="name cannot be empty"):
            Dataset(name="", source="s3://bucket/file.parquet")

    @pytest.mark.core
    def test_dataset_empty_source_raises(self) -> None:
        """Dataset raises ValueError for empty source."""
        with pytest.raises(ValueError, match="source cannot be empty"):
            Dataset(name="test", source="")

    @pytest.mark.core
    def test_dataset_with_cache_path(self) -> None:
        """with_cache_path returns a new Dataset with updated path."""
        original = Dataset(name="test", source="s3://bucket/file.parquet")
        new_path = Path("/data/file.parquet")

        updated = original.with_cache_path(new_path)

        assert updated.cache_path == new_path
        assert updated.name == original.name
        assert updated.source == original.source
        assert original.cache_path is None  # Original unchanged


class TestFileMetadata:
    """Tests for the FileMetadata model."""

    @pytest.mark.core
    def test_file_metadata_with_etag(self) -> None:
        """FileMetadata can be created with just etag."""
        meta = FileMetadata(etag="abc123")

        assert meta.etag == "abc123"
        assert meta.last_modified is None
        assert meta.size is None

    @pytest.mark.core
    def test_file_metadata_with_last_modified(self) -> None:
        """FileMetadata can be created with just last_modified."""
        now = datetime.now()
        meta = FileMetadata(last_modified=now)

        assert meta.etag is None
        assert meta.last_modified == now

    @pytest.mark.core
    def test_file_metadata_with_all_fields(self) -> None:
        """FileMetadata can be created with all fields."""
        now = datetime.now()
        meta = FileMetadata(etag="abc123", last_modified=now, size=1024)

        assert meta.etag == "abc123"
        assert meta.last_modified == now
        assert meta.size == 1024

    @pytest.mark.core
    def test_file_metadata_requires_staleness_indicator(self) -> None:
        """FileMetadata raises ValueError if no staleness indicator provided."""
        with pytest.raises(ValueError, match="at least etag or last_modified"):
            FileMetadata()

    @pytest.mark.core
    def test_file_metadata_matches_same_etag(self) -> None:
        """FileMetadata.matches returns True for same etag."""
        meta1 = FileMetadata(etag="abc123")
        meta2 = FileMetadata(etag="abc123")

        assert meta1.matches(meta2)
        assert meta2.matches(meta1)

    @pytest.mark.core
    def test_file_metadata_matches_different_etag(self) -> None:
        """FileMetadata.matches returns False for different etag."""
        meta1 = FileMetadata(etag="abc123")
        meta2 = FileMetadata(etag="xyz789")

        assert not meta1.matches(meta2)

    @pytest.mark.core
    def test_file_metadata_matches_same_last_modified(self) -> None:
        """FileMetadata.matches returns True for same last_modified."""
        now = datetime.now()
        meta1 = FileMetadata(last_modified=now)
        meta2 = FileMetadata(last_modified=now)

        assert meta1.matches(meta2)

    @pytest.mark.core
    def test_file_metadata_matches_different_last_modified(self) -> None:
        """FileMetadata.matches returns False for different last_modified."""
        now = datetime.now()
        later = now + timedelta(seconds=1)
        meta1 = FileMetadata(last_modified=now)
        meta2 = FileMetadata(last_modified=later)

        assert not meta1.matches(meta2)

    @pytest.mark.core
    def test_file_metadata_etag_takes_precedence(self) -> None:
        """FileMetadata.matches uses etag over last_modified."""
        now = datetime.now()
        later = now + timedelta(seconds=1)

        # Same etag but different timestamps should match
        meta1 = FileMetadata(etag="abc123", last_modified=now)
        meta2 = FileMetadata(etag="abc123", last_modified=later)

        assert meta1.matches(meta2)

    @pytest.mark.core
    def test_file_metadata_immutable(self) -> None:
        """FileMetadata is immutable (frozen dataclass)."""
        meta = FileMetadata(etag="abc123")

        with pytest.raises(AttributeError):
            meta.etag = "new"  # type: ignore[misc]


class TestCacheMetadata:
    """Tests for the CacheMetadata model."""

    @pytest.mark.core
    def test_cache_metadata_creation(self) -> None:
        """CacheMetadata can be created with etag."""
        meta = CacheMetadata(etag="abc123", source="s3://bucket/file.parquet")

        assert meta.etag == "abc123"
        assert meta.source == "s3://bucket/file.parquet"
        assert meta.cached_at is not None

    @pytest.mark.core
    def test_cache_metadata_to_file_metadata(self) -> None:
        """CacheMetadata can be converted to FileMetadata."""
        now = datetime.now()
        cache_meta = CacheMetadata(
            etag="abc123",
            last_modified=now,
            source="s3://bucket/file.parquet",
        )

        file_meta = cache_meta.to_file_metadata()

        assert file_meta.etag == "abc123"
        assert file_meta.last_modified == now

    @pytest.mark.core
    def test_cache_metadata_to_file_metadata_raises_without_indicators(self) -> None:
        """to_file_metadata raises if no staleness indicators."""
        cache_meta = CacheMetadata(source="s3://bucket/file.parquet")

        with pytest.raises(ValueError):
            cache_meta.to_file_metadata()

    @pytest.mark.core
    def test_cache_metadata_is_stale_false_when_matches(self) -> None:
        """is_stale returns False when cache matches remote."""
        cache_meta = CacheMetadata(etag="abc123", source="s3://bucket/file.parquet")
        remote_meta = FileMetadata(etag="abc123")

        assert not cache_meta.is_stale(remote_meta)

    @pytest.mark.core
    def test_cache_metadata_is_stale_true_when_different(self) -> None:
        """is_stale returns True when cache differs from remote."""
        cache_meta = CacheMetadata(etag="abc123", source="s3://bucket/file.parquet")
        remote_meta = FileMetadata(etag="xyz789")

        assert cache_meta.is_stale(remote_meta)

    @pytest.mark.core
    def test_cache_metadata_immutable(self) -> None:
        """CacheMetadata is immutable (frozen dataclass)."""
        meta = CacheMetadata(etag="abc123")

        with pytest.raises(AttributeError):
            meta.etag = "new"  # type: ignore[misc]
