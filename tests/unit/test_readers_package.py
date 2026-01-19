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
        """__all__ should list all 5 reader classes."""
        from datacachalog.adapters import readers

        expected = {
            "GeoParquetReader",
            "PandasCsvReader",
            "PandasParquetReader",
            "PolarsCsvReader",
            "PolarsParquetReader",
        }
        assert set(readers.__all__) == expected

    @pytest.mark.tier(0)
    @pytest.mark.tra("Adapter.Readers.ExportsFromPackage")
    def test_geoparquet_reader_exported_from_readers_package(self) -> None:
        """GeoParquetReader should be importable from adapters.readers."""
        from datacachalog.adapters.readers import GeoParquetReader

        # Verify we got the class, not None
        assert GeoParquetReader is not None

        # Verify it has read method
        assert hasattr(GeoParquetReader, "read")
