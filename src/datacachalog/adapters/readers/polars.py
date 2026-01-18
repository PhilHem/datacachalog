"""Polars readers for Parquet and CSV files.

These readers transform cached files into polars DataFrames by wrapping
polars.read_parquet() and polars.read_csv().
"""

from pathlib import Path

import polars as pl


class PolarsParquetReader:
    """Reader that loads Parquet files into polars DataFrames."""

    def read(self, path: Path) -> pl.DataFrame:
        """Load a Parquet file using polars.read_parquet().

        Args:
            path: Path to the cached Parquet file.

        Returns:
            A polars DataFrame containing the file data.

        Raises:
            FileNotFoundError: If the file does not exist.
            polars.exceptions.ComputeError: If the file is not valid Parquet.
        """
        return pl.read_parquet(path)


class PolarsCsvReader:
    """Reader that loads CSV files into polars DataFrames."""

    def read(self, path: Path) -> pl.DataFrame:
        """Load a CSV file using polars.read_csv().

        Args:
            path: Path to the cached CSV file.

        Returns:
            A polars DataFrame containing the file data.

        Raises:
            FileNotFoundError: If the file does not exist.
            polars.exceptions.ComputeError: If the file is not valid CSV.
        """
        return pl.read_csv(path)
