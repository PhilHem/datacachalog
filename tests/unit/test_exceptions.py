"""Unit tests for domain exception hierarchy."""

import pytest


@pytest.mark.core
class TestDatacachalogError:
    """Tests for base exception class."""

    def test_is_exception_subclass(self) -> None:
        """DatacachalogError should be an Exception subclass."""
        from datacachalog.core.exceptions import DatacachalogError

        assert issubclass(DatacachalogError, Exception)

    def test_recovery_hint_returns_none_by_default(self) -> None:
        """Base exception should return None for recovery_hint."""
        from datacachalog.core.exceptions import DatacachalogError

        err = DatacachalogError("something went wrong")
        assert err.recovery_hint is None


@pytest.mark.core
class TestDatasetNotFoundError:
    """Tests for DatasetNotFoundError."""

    def test_is_datacachalog_error_subclass(self) -> None:
        """DatasetNotFoundError should inherit from DatacachalogError."""
        from datacachalog.core.exceptions import (
            DatacachalogError,
            DatasetNotFoundError,
        )

        assert issubclass(DatasetNotFoundError, DatacachalogError)

    def test_stores_dataset_name(self) -> None:
        """Exception should store the missing dataset name."""
        from datacachalog.core.exceptions import DatasetNotFoundError

        err = DatasetNotFoundError("customers")
        assert err.name == "customers"

    def test_message_includes_dataset_name(self) -> None:
        """Exception message should include the dataset name."""
        from datacachalog.core.exceptions import DatasetNotFoundError

        err = DatasetNotFoundError("customers")
        assert "customers" in str(err)

    def test_stores_available_datasets(self) -> None:
        """Exception should store list of available datasets."""
        from datacachalog.core.exceptions import DatasetNotFoundError

        err = DatasetNotFoundError("missing", available=["alpha", "beta"])
        assert err.available == ["alpha", "beta"]

    def test_available_defaults_to_empty_list(self) -> None:
        """Available datasets should default to empty list."""
        from datacachalog.core.exceptions import DatasetNotFoundError

        err = DatasetNotFoundError("missing")
        assert err.available == []

    def test_recovery_hint_includes_available_datasets(self) -> None:
        """recovery_hint should list available dataset names."""
        from datacachalog.core.exceptions import DatasetNotFoundError

        err = DatasetNotFoundError("missing", available=["alpha", "beta"])
        hint = err.recovery_hint
        assert hint is not None
        assert "alpha" in hint
        assert "beta" in hint

    def test_recovery_hint_when_no_available_datasets(self) -> None:
        """recovery_hint should provide guidance even with no available datasets."""
        from datacachalog.core.exceptions import DatasetNotFoundError

        err = DatasetNotFoundError("missing", available=[])
        hint = err.recovery_hint
        assert hint is not None
        assert "datasets" in hint.lower()


@pytest.mark.core
class TestStorageError:
    """Tests for StorageError base class."""

    def test_is_datacachalog_error_subclass(self) -> None:
        """StorageError should inherit from DatacachalogError."""
        from datacachalog.core.exceptions import DatacachalogError, StorageError

        assert issubclass(StorageError, DatacachalogError)

    def test_stores_message_and_source(self) -> None:
        """StorageError should store message and source."""
        from datacachalog.core.exceptions import StorageError

        err = StorageError("Operation failed", source="s3://bucket/file.csv")
        assert "Operation failed" in str(err)
        assert err.source == "s3://bucket/file.csv"

    def test_stores_cause(self) -> None:
        """StorageError should store the underlying cause."""
        from datacachalog.core.exceptions import StorageError

        cause = ValueError("original error")
        err = StorageError("Wrapped", source="/path", cause=cause)
        assert err.cause is cause

    def test_cause_defaults_to_none(self) -> None:
        """cause should default to None."""
        from datacachalog.core.exceptions import StorageError

        err = StorageError("Message", source="/path")
        assert err.cause is None


@pytest.mark.core
class TestStorageNotFoundError:
    """Tests for StorageNotFoundError."""

    def test_is_storage_error_subclass(self) -> None:
        """StorageNotFoundError should inherit from StorageError."""
        from datacachalog.core.exceptions import StorageError, StorageNotFoundError

        assert issubclass(StorageNotFoundError, StorageError)

    def test_recovery_hint_includes_source(self) -> None:
        """recovery_hint should mention the source path."""
        from datacachalog.core.exceptions import StorageNotFoundError

        err = StorageNotFoundError("Not found", source="s3://bucket/missing.csv")
        hint = err.recovery_hint
        assert hint is not None
        assert "s3://bucket/missing.csv" in hint


@pytest.mark.core
class TestStorageAccessError:
    """Tests for StorageAccessError."""

    def test_is_storage_error_subclass(self) -> None:
        """StorageAccessError should inherit from StorageError."""
        from datacachalog.core.exceptions import StorageAccessError, StorageError

        assert issubclass(StorageAccessError, StorageError)

    def test_recovery_hint_mentions_permissions(self) -> None:
        """recovery_hint should mention permissions or credentials."""
        from datacachalog.core.exceptions import StorageAccessError

        err = StorageAccessError("Access denied", source="s3://bucket/file.csv")
        hint = err.recovery_hint
        assert hint is not None
        assert "permission" in hint.lower() or "credential" in hint.lower()


@pytest.mark.core
class TestCacheError:
    """Tests for CacheError base class."""

    def test_is_datacachalog_error_subclass(self) -> None:
        """CacheError should inherit from DatacachalogError."""
        from datacachalog.core.exceptions import CacheError, DatacachalogError

        assert issubclass(CacheError, DatacachalogError)


@pytest.mark.core
class TestCacheCorruptError:
    """Tests for CacheCorruptError."""

    def test_is_cache_error_subclass(self) -> None:
        """CacheCorruptError should inherit from CacheError."""
        from datacachalog.core.exceptions import CacheCorruptError, CacheError

        assert issubclass(CacheCorruptError, CacheError)

    def test_stores_key_and_path(self) -> None:
        """CacheCorruptError should store the cache key and path."""
        from pathlib import Path

        from datacachalog.core.exceptions import CacheCorruptError

        err = CacheCorruptError(
            "Corrupt", key="customers", path=Path("/cache/customers")
        )
        assert err.key == "customers"
        assert err.path == Path("/cache/customers")

    def test_recovery_hint_mentions_delete(self) -> None:
        """recovery_hint should suggest deleting corrupt files."""
        from pathlib import Path

        from datacachalog.core.exceptions import CacheCorruptError

        err = CacheCorruptError(
            "Corrupt", key="customers", path=Path("/cache/customers")
        )
        hint = err.recovery_hint
        assert hint is not None
        assert "delete" in hint.lower() or "re-fetch" in hint.lower()


@pytest.mark.core
class TestConfigurationError:
    """Tests for ConfigurationError."""

    def test_is_datacachalog_error_subclass(self) -> None:
        """ConfigurationError should inherit from DatacachalogError."""
        from datacachalog.core.exceptions import ConfigurationError, DatacachalogError

        assert issubclass(ConfigurationError, DatacachalogError)
