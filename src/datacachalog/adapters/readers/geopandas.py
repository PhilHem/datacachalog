"""GeoPandas reader adapter for GeoParquet files.

Provides Reader implementation that loads cached GeoParquet files
into geopandas GeoDataFrames.
"""

from pathlib import Path

import geopandas as gpd


class GeoParquetReader:
    """Reader adapter for GeoParquet files using geopandas.

    Wraps gpd.read_parquet() to satisfy the Reader[gpd.GeoDataFrame] protocol.
    Preserves geometry column, CRS, and all spatial information from the file.

    Args:
        target_crs: Optional CRS to reproject data to after reading.
                   If None, preserves original CRS from file.
        columns: Optional list of column names to read from the file.
                If None, reads all columns. Geometry column is always preserved
                even if not explicitly included in the columns list.
        bbox: Optional bounding box (minx, miny, maxx, maxy) for spatial filtering.
             If specified, only features intersecting the bbox are returned.
    """

    def __init__(
        self,
        target_crs: str | None = None,
        columns: list[str] | None = None,
        bbox: tuple[float, float, float, float] | None = None,
    ) -> None:
        """Initialize GeoParquetReader with optional target CRS, columns filter, and bbox.

        Args:
            target_crs: CRS string (e.g., "EPSG:3857") to reproject to.
                       If None, no reprojection is performed.
            columns: List of column names to read. If None, reads all columns.
                    Geometry column is always included even if not in the list.
            bbox: Bounding box tuple (minx, miny, maxx, maxy) for spatial filtering.
                 If None, no spatial filtering is applied.
        """
        self.target_crs = target_crs
        self.columns = columns
        self.bbox = bbox

    def read(self, path: Path) -> gpd.GeoDataFrame:
        """Load a GeoParquet file into a geopandas GeoDataFrame.

        Args:
            path: Path to the cached GeoParquet file.

        Returns:
            geopandas GeoDataFrame with the loaded data and geometry.
            If target_crs was specified, data will be reprojected.
            If columns were specified, only those columns will be loaded
            (geometry is always preserved).
            If bbox was specified, only features intersecting the bbox are returned.

        Raises:
            FileNotFoundError: If the file does not exist.
            pyproj.exceptions.CRSError: If target_crs is invalid.
            Exception: Any geopandas-specific exceptions (e.g., ParserError).
        """
        # If columns filter is specified, ensure geometry column is included
        columns = self.columns
        if columns is not None and "geometry" not in columns:
            columns = [*columns, "geometry"]

        # Try to use bbox parameter for efficient filtering at read time
        # This only works if the parquet file has proper spatial encoding
        try:
            gdf = gpd.read_parquet(path, columns=columns, bbox=self.bbox)
        except ValueError as e:
            # If bbox filtering not supported by file encoding, fall back to
            # reading full file and filtering in memory
            if "bbox" in str(e) and self.bbox is not None:
                gdf = gpd.read_parquet(path, columns=columns)
                # Filter by creating a bounding box geometry and using cx indexer
                minx, miny, maxx, maxy = self.bbox
                gdf = gdf.cx[minx:maxx, miny:maxy]  # type: ignore[misc]
            else:
                raise

        if self.target_crs is not None:
            gdf = gdf.to_crs(self.target_crs)

        return gdf
