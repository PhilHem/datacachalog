"""Reader adapters for loading cached files into typed objects.

This package provides adapters that implement the Reader protocol for
various data formats and libraries:

- Polars: PolarsParquetReader, PolarsCsvReader
- Pandas: PandasParquetReader, PandasCsvReader
- GeoPandas: GeoParquetReader
"""

from datacachalog.adapters.readers.geopandas import GeoParquetReader
from datacachalog.adapters.readers.pandas import PandasCsvReader, PandasParquetReader
from datacachalog.adapters.readers.polars import PolarsCsvReader, PolarsParquetReader


__all__ = [
    "GeoParquetReader",
    "PandasCsvReader",
    "PandasParquetReader",
    "PolarsCsvReader",
    "PolarsParquetReader",
]
