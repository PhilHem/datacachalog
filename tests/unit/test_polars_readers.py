"""Unit tests for Polars readers.

Tests Protocol compliance and data integrity for PolarsParquetReader and PolarsCsvReader.
"""

from pathlib import Path

import polars as pl
import pytest

from datacachalog.core.ports import Reader


@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Sample DataFrame for testing readers."""
    return pl.DataFrame(
        {
            "id": [1, 2, 3],
            "name": ["Alice", "Bob", "Charlie"],
            "value": [10.5, 20.3, 30.1],
        }
    )


def _assert_dataframe_matches_sample(df: pl.DataFrame) -> None:
    """Helper to verify DataFrame matches expected sample data."""
    assert isinstance(df, pl.DataFrame)
    assert df.shape == (3, 3)
    assert df["id"].to_list() == [1, 2, 3]
    assert df["name"].to_list() == ["Alice", "Bob", "Charlie"]
    assert df["value"].to_list() == [10.5, 20.3, 30.1]


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Reader.PolarsParquet.ProtocolCompliance")
def test_polars_parquet_reader_satisfies_reader_protocol() -> None:
    """Verify PolarsParquetReader satisfies Reader protocol."""
    from datacachalog.adapters.readers.polars import PolarsParquetReader

    reader = PolarsParquetReader()
    assert isinstance(reader, Reader)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Reader.PolarsParquet.DataIntegrity")
def test_polars_parquet_reader_reads_correct_data(
    tmp_path: Path, sample_dataframe: pl.DataFrame
) -> None:
    """Verify PolarsParquetReader reads Parquet files correctly."""
    from datacachalog.adapters.readers.polars import PolarsParquetReader

    # Create test parquet file
    parquet_path = tmp_path / "test.parquet"
    sample_dataframe.write_parquet(parquet_path)

    # Read with reader
    reader = PolarsParquetReader()
    result = reader.read(parquet_path)

    # Verify data integrity
    _assert_dataframe_matches_sample(result)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Reader.PolarsCSV.ProtocolCompliance")
def test_polars_csv_reader_satisfies_reader_protocol() -> None:
    """Verify PolarsCsvReader satisfies Reader protocol."""
    from datacachalog.adapters.readers.polars import PolarsCsvReader

    reader = PolarsCsvReader()
    assert isinstance(reader, Reader)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Reader.PolarsCSV.DataIntegrity")
def test_polars_csv_reader_reads_correct_data(
    tmp_path: Path, sample_dataframe: pl.DataFrame
) -> None:
    """Verify PolarsCsvReader reads CSV files correctly."""
    from datacachalog.adapters.readers.polars import PolarsCsvReader

    # Create test CSV file
    csv_path = tmp_path / "test.csv"
    sample_dataframe.write_csv(csv_path)

    # Read with reader
    reader = PolarsCsvReader()
    result = reader.read(csv_path)

    # Verify data integrity
    _assert_dataframe_matches_sample(result)
