"""Unit tests for Pandas reader adapters.

Tests the PandasParquetReader and PandasCsvReader implementations
of the Reader protocol for loading cached files into pandas DataFrames.
"""

from pathlib import Path

import pandas as pd
import pytest

from datacachalog.adapters.readers.pandas import PandasCsvReader, PandasParquetReader
from datacachalog.core.ports import Reader


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.PandasParquetReader.ProtocolCompliance")
def test_pandas_parquet_reader_satisfies_reader_protocol() -> None:
    """PandasParquetReader satisfies Reader protocol."""
    reader = PandasParquetReader()
    assert isinstance(reader, Reader)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.PandasParquetReader.DataIntegrity")
def test_pandas_parquet_reader_reads_correct_data(tmp_path: Path) -> None:
    """PandasParquetReader reads Parquet files with correct data."""
    # Arrange: Create test Parquet file
    test_file = tmp_path / "test.parquet"
    expected_df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    expected_df.to_parquet(test_file)

    # Act: Read with PandasParquetReader
    reader = PandasParquetReader()
    result_df = reader.read(test_file)

    # Assert: Data matches
    pd.testing.assert_frame_equal(result_df, expected_df)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.PandasCsvReader.ProtocolCompliance")
def test_pandas_csv_reader_satisfies_reader_protocol() -> None:
    """PandasCsvReader satisfies Reader protocol."""
    reader = PandasCsvReader()
    assert isinstance(reader, Reader)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.PandasCsvReader.DataIntegrity")
def test_pandas_csv_reader_reads_correct_data(tmp_path: Path) -> None:
    """PandasCsvReader reads CSV files with correct data."""
    # Arrange: Create test CSV file
    test_file = tmp_path / "test.csv"
    expected_df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    expected_df.to_csv(test_file, index=False)

    # Act: Read with PandasCsvReader
    reader = PandasCsvReader()
    result_df = reader.read(test_file)

    # Assert: Data matches
    pd.testing.assert_frame_equal(result_df, expected_df)
