"""Unit tests for core domain models.

These tests verify the behavior of Dataset, FileMetadata, and CacheMetadata.
They are pure unit tests with no I/O dependencies.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from datacachalog.core.models import CacheMetadata, Dataset, FileMetadata, ObjectVersion


@pytest.mark.tra("Domain.Dataset")
@pytest.mark.tier(0)
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


@pytest.mark.tra("Domain.Dataset")
@pytest.mark.tier(0)
class TestDatasetReader:
    """Tests for Dataset.reader field."""

    @pytest.mark.core
    def test_dataset_reader_defaults_to_none(self) -> None:
        """Dataset.reader defaults to None when not provided."""
        dataset = Dataset(name="test", source="s3://bucket/file.parquet")

        assert dataset.reader is None

    @pytest.mark.core
    def test_dataset_with_reader(self) -> None:
        """Dataset can be created with a reader."""
        from datacachalog.core.ports import Reader

        class StringReader:
            def read(self, path: Path) -> str:
                return "data"

        reader: Reader[str] = StringReader()
        dataset = Dataset(
            name="test",
            source="s3://bucket/file.parquet",
            reader=reader,
        )

        assert dataset.reader is reader

    @pytest.mark.core
    def test_dataset_reader_preserved_in_with_cache_path(self) -> None:
        """with_cache_path preserves the reader field."""
        from datacachalog.core.ports import Reader

        class StringReader:
            def read(self, path: Path) -> str:
                return "data"

        reader: Reader[str] = StringReader()
        original = Dataset(
            name="test",
            source="s3://bucket/file.parquet",
            reader=reader,
        )

        updated = original.with_cache_path(Path("/cache/file.parquet"))

        assert updated.reader is reader
        assert updated.cache_path == Path("/cache/file.parquet")

    @pytest.mark.core
    def test_dataset_reader_preserved_in_with_resolved_paths(self) -> None:
        """with_resolved_paths preserves the reader field."""
        from datacachalog.core.ports import Reader

        class StringReader:
            def read(self, path: Path) -> str:
                return "data"

        reader: Reader[str] = StringReader()
        original = Dataset(
            name="test",
            source="s3://bucket/file.parquet",
            cache_path=Path("data/file.parquet"),
            reader=reader,
        )

        resolved = original.with_resolved_paths(Path("/project"))

        assert resolved.reader is reader
        assert resolved.cache_path == Path("/project/data/file.parquet")


@pytest.mark.tra("Domain.Dataset")
@pytest.mark.tier(0)
class TestDatasetWithResolvedPaths:
    """Tests for Dataset.with_resolved_paths()."""

    @pytest.mark.core
    def test_resolves_relative_cache_path(self) -> None:
        """Relative cache_path is resolved against root."""
        dataset = Dataset(
            name="test",
            source="s3://bucket/file.parquet",
            cache_path=Path("data/file.parquet"),
        )
        root = Path("/project")

        resolved = dataset.with_resolved_paths(root)

        assert resolved.cache_path == Path("/project/data/file.parquet")

    @pytest.mark.core
    def test_preserves_absolute_cache_path(self) -> None:
        """Absolute cache_path is unchanged."""
        dataset = Dataset(
            name="test",
            source="s3://bucket/file.parquet",
            cache_path=Path("/absolute/path/file.parquet"),
        )
        root = Path("/project")

        resolved = dataset.with_resolved_paths(root)

        assert resolved.cache_path == Path("/absolute/path/file.parquet")

    @pytest.mark.core
    def test_preserves_none_cache_path(self) -> None:
        """None cache_path remains None."""
        dataset = Dataset(name="test", source="s3://bucket/file.parquet")
        root = Path("/project")

        resolved = dataset.with_resolved_paths(root)

        assert resolved.cache_path is None

    @pytest.mark.core
    def test_preserves_other_fields(self) -> None:
        """Name, source, description are preserved."""
        dataset = Dataset(
            name="customers",
            source="s3://bucket/customers.parquet",
            description="Customer data",
            cache_path=Path("data/customers.parquet"),
        )
        root = Path("/project")

        resolved = dataset.with_resolved_paths(root)

        assert resolved.name == "customers"
        assert resolved.source == "s3://bucket/customers.parquet"
        assert resolved.description == "Customer data"

    @pytest.mark.core
    def test_returns_new_instance(self) -> None:
        """Original dataset is unchanged (immutability)."""
        original = Dataset(
            name="test",
            source="s3://bucket/file.parquet",
            cache_path=Path("data/file.parquet"),
        )
        root = Path("/project")

        resolved = original.with_resolved_paths(root)

        assert resolved is not original
        assert original.cache_path == Path("data/file.parquet")


@pytest.mark.tra("Domain.FileMetadata")
@pytest.mark.tier(0)
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


@pytest.mark.tra("Domain.CacheMetadata")
@pytest.mark.tier(0)
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


@pytest.mark.tra("Domain.ObjectVersion")
@pytest.mark.tier(0)
class TestObjectVersion:
    """Tests for the ObjectVersion model."""

    @pytest.mark.core
    def test_object_version_creation_full(self) -> None:
        """ObjectVersion stores version metadata."""
        version = ObjectVersion(
            version_id="abc123",
            last_modified=datetime(2024, 12, 14, 9, 30, 0),
            etag='"xyz789"',
            size=1024,
            is_latest=True,
            is_delete_marker=False,
        )

        assert version.version_id == "abc123"
        assert version.last_modified == datetime(2024, 12, 14, 9, 30, 0)
        assert version.etag == '"xyz789"'
        assert version.size == 1024
        assert version.is_latest is True
        assert version.is_delete_marker is False

    @pytest.mark.core
    def test_object_version_minimal(self) -> None:
        """ObjectVersion requires only last_modified."""
        version = ObjectVersion(last_modified=datetime(2024, 12, 14))

        assert version.version_id is None
        assert version.etag is None
        assert version.size is None
        assert version.is_latest is False
        assert version.is_delete_marker is False

    @pytest.mark.core
    def test_object_version_is_frozen(self) -> None:
        """ObjectVersion is immutable."""
        version = ObjectVersion(last_modified=datetime.now())

        with pytest.raises(AttributeError):
            version.is_latest = False  # type: ignore[misc]

    @pytest.mark.core
    def test_object_version_requires_last_modified(self) -> None:
        """ObjectVersion must have last_modified."""
        with pytest.raises(TypeError):
            ObjectVersion()  # type: ignore[call-arg]

    @pytest.mark.core
    def test_object_versions_sort_by_last_modified(self) -> None:
        """Versions sort by last_modified descending (newest first)."""
        v1 = ObjectVersion(last_modified=datetime(2024, 12, 10))
        v2 = ObjectVersion(last_modified=datetime(2024, 12, 14))
        v3 = ObjectVersion(last_modified=datetime(2024, 12, 12))

        versions = sorted([v1, v2, v3], reverse=True)

        assert versions == [v2, v3, v1]

    @pytest.mark.core
    def test_object_version_to_file_metadata(self) -> None:
        """ObjectVersion converts to FileMetadata for staleness checks."""
        version = ObjectVersion(
            last_modified=datetime(2024, 12, 14),
            etag='"abc123"',
            size=2048,
        )

        metadata = version.to_file_metadata()

        assert metadata.etag == '"abc123"'
        assert metadata.last_modified == datetime(2024, 12, 14)
        assert metadata.size == 2048

    @pytest.mark.core
    def test_object_version_to_file_metadata_minimal(self) -> None:
        """ObjectVersion with only last_modified converts to FileMetadata."""
        version = ObjectVersion(last_modified=datetime(2024, 12, 14))

        metadata = version.to_file_metadata()

        assert metadata.last_modified == datetime(2024, 12, 14)
        assert metadata.etag is None


@pytest.mark.tra("Domain.ObjectVersion")
@pytest.mark.tier(0)
class TestFindVersionAt:
    """Tests for the find_version_at() pure function."""

    @pytest.mark.core
    def test_find_version_at_returns_closest_before(self) -> None:
        """find_version_at() returns version with last_modified <= as_of."""
        from datacachalog.core.models import find_version_at

        versions = [
            ObjectVersion(last_modified=datetime(2024, 12, 15), version_id="v3"),
            ObjectVersion(last_modified=datetime(2024, 12, 10), version_id="v2"),
            ObjectVersion(last_modified=datetime(2024, 12, 5), version_id="v1"),
        ]

        result = find_version_at(versions, as_of=datetime(2024, 12, 12))

        assert result is not None
        assert result.version_id == "v2"  # Dec 10 is closest to Dec 12

    @pytest.mark.core
    def test_find_version_at_exact_match(self) -> None:
        """Exact datetime match returns that version."""
        from datacachalog.core.models import find_version_at

        versions = [
            ObjectVersion(last_modified=datetime(2024, 12, 15), version_id="v3"),
            ObjectVersion(last_modified=datetime(2024, 12, 10), version_id="v2"),
            ObjectVersion(last_modified=datetime(2024, 12, 5), version_id="v1"),
        ]

        result = find_version_at(versions, as_of=datetime(2024, 12, 10))

        assert result is not None
        assert result.version_id == "v2"

    @pytest.mark.core
    def test_find_version_at_returns_none_if_all_after(self) -> None:
        """Returns None if all versions are after as_of."""
        from datacachalog.core.models import find_version_at

        versions = [
            ObjectVersion(last_modified=datetime(2024, 12, 15), version_id="v3"),
            ObjectVersion(last_modified=datetime(2024, 12, 10), version_id="v2"),
        ]

        result = find_version_at(versions, as_of=datetime(2024, 12, 1))

        assert result is None

    @pytest.mark.core
    def test_find_version_at_skips_delete_markers(self) -> None:
        """Delete markers are skipped during resolution."""
        from datacachalog.core.models import find_version_at

        versions = [
            ObjectVersion(
                last_modified=datetime(2024, 12, 15),
                version_id="v3",
                is_delete_marker=True,
            ),
            ObjectVersion(last_modified=datetime(2024, 12, 10), version_id="v2"),
            ObjectVersion(last_modified=datetime(2024, 12, 5), version_id="v1"),
        ]

        # Dec 15 delete marker should be skipped, returns Dec 10
        result = find_version_at(versions, as_of=datetime(2024, 12, 20))

        assert result is not None
        assert result.version_id == "v2"

    @pytest.mark.core
    def test_find_version_at_empty_list(self) -> None:
        """Returns None for empty version list."""
        from datacachalog.core.models import find_version_at

        result = find_version_at([], as_of=datetime(2024, 12, 10))

        assert result is None

    @pytest.mark.core
    def test_find_version_at_all_delete_markers(self) -> None:
        """Returns None if all versions are delete markers."""
        from datacachalog.core.models import find_version_at

        versions = [
            ObjectVersion(
                last_modified=datetime(2024, 12, 15),
                version_id="v2",
                is_delete_marker=True,
            ),
            ObjectVersion(
                last_modified=datetime(2024, 12, 10),
                version_id="v1",
                is_delete_marker=True,
            ),
        ]

        result = find_version_at(versions, as_of=datetime(2024, 12, 20))

        assert result is None
