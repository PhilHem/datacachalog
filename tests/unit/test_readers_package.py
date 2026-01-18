"""Tests for readers package exports."""

import pytest


@pytest.mark.core
class TestReadersPackageExports:
    """Verify all readers are exported from the package."""

    @pytest.mark.tier(0)
    @pytest.mark.tra("Adapter.Readers.ExportsFromPackage")
    def test_all_readers_exported_from_readers_package(self) -> None:
        """All 4 reader classes should be importable from adapters.readers."""
        from datacachalog.adapters.readers import (
            PandasCsvReader,
            PandasParquetReader,
            PolarsCsvReader,
            PolarsParquetReader,
        )

        # Verify we got actual classes, not None
        assert PandasCsvReader is not None
        assert PandasParquetReader is not None
        assert PolarsCsvReader is not None
        assert PolarsParquetReader is not None

        # Verify they have read methods
        assert hasattr(PandasCsvReader, "read")
        assert hasattr(PandasParquetReader, "read")
        assert hasattr(PolarsCsvReader, "read")
        assert hasattr(PolarsParquetReader, "read")

    @pytest.mark.tier(0)
    @pytest.mark.tra("Adapter.Readers.DunderAllCorrect")
    def test_dunder_all_contains_all_readers(self) -> None:
        """__all__ should list all 4 reader classes."""
        from datacachalog.adapters import readers

        expected = {
            "PandasCsvReader",
            "PandasParquetReader",
            "PolarsCsvReader",
            "PolarsParquetReader",
        }
        assert set(readers.__all__) == expected
