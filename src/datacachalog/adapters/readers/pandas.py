"""Pandas reader adapters for CSV and Parquet files.

Provides Reader implementations that load cached files into pandas DataFrames.
"""

from pathlib import Path

import pandas as pd


class PandasParquetReader:
    """Reader adapter for Parquet files using pandas.

    Wraps pd.read_parquet() to satisfy the Reader[pd.DataFrame] protocol.
    """

    def read(self, path: Path) -> pd.DataFrame:
        """Load a Parquet file into a pandas DataFrame.

        Args:
            path: Path to the cached Parquet file.

        Returns:
            pandas DataFrame with the loaded data.

        Raises:
            FileNotFoundError: If the file does not exist.
            Exception: Any pandas-specific exceptions (e.g., ParserError).
        """
        return pd.read_parquet(path)


class PandasCsvReader:
    """Reader adapter for CSV files using pandas.

    Wraps pd.read_csv() to satisfy the Reader[pd.DataFrame] protocol.
    """

    def read(self, path: Path) -> pd.DataFrame:
        """Load a CSV file into a pandas DataFrame.

        Args:
            path: Path to the cached CSV file.

        Returns:
            pandas DataFrame with the loaded data.

        Raises:
            FileNotFoundError: If the file does not exist.
            Exception: Any pandas-specific exceptions (e.g., ParserError).
        """
        return pd.read_csv(path)
