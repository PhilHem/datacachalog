"""Unit tests for GeoParquet reader adapter.

Tests the GeoParquetReader implementation of the Reader protocol
for loading cached GeoParquet files into geopandas GeoDataFrames.
"""

from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import Point

from datacachalog.adapters.readers.geopandas import GeoParquetReader
from datacachalog.core.ports import Reader


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.ProtocolCompliance")
def test_geoparquet_reader_satisfies_reader_protocol() -> None:
    """GeoParquetReader satisfies Reader protocol."""
    reader = GeoParquetReader()
    assert isinstance(reader, Reader)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.DataIntegrity")
def test_geoparquet_reader_reads_valid_file(tmp_path: Path) -> None:
    """GeoParquetReader reads GeoParquet files with correct data."""
    # Arrange: Create test GeoParquet file
    test_file = tmp_path / "test.parquet"
    expected_gdf = gpd.GeoDataFrame(
        {"id": [1, 2, 3], "name": ["a", "b", "c"]},
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:4326",
    )
    expected_gdf.to_parquet(test_file)

    # Act: Read with GeoParquetReader
    reader = GeoParquetReader()
    result_gdf = reader.read(test_file)

    # Assert: Data matches
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    assert len(result_gdf) == 3
    assert list(result_gdf["id"]) == [1, 2, 3]
    assert list(result_gdf["name"]) == ["a", "b", "c"]


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.GeometryPreservation")
def test_geoparquet_reader_preserves_geometry(tmp_path: Path) -> None:
    """GeoParquetReader preserves geometry column correctly."""
    # Arrange: Create test GeoParquet file with Point geometries
    test_file = tmp_path / "test_geom.parquet"
    geometries = [Point(0, 0), Point(1, 1), Point(2, 2)]
    expected_gdf = gpd.GeoDataFrame(
        {"id": [1, 2, 3]},
        geometry=geometries,
        crs="EPSG:4326",
    )
    expected_gdf.to_parquet(test_file)

    # Act: Read with GeoParquetReader
    reader = GeoParquetReader()
    result_gdf = reader.read(test_file)

    # Verify geometry column preserved
    assert result_gdf.geometry.name == "geometry"
    assert all(result_gdf.geometry == expected_gdf.geometry)
    for i, geom in enumerate(result_gdf.geometry):
        expected_geom = geometries[i]
        assert geom.equals(expected_geom)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.CrsPreservation")
def test_geoparquet_reader_preserves_crs(tmp_path: Path) -> None:
    """GeoParquetReader preserves coordinate reference system."""
    # Arrange: Create test GeoParquet file with specific CRS
    test_file = tmp_path / "test_crs.parquet"
    crs = "EPSG:3857"  # Web Mercator
    expected_gdf = gpd.GeoDataFrame(
        {"id": [1, 2]},
        geometry=[Point(0, 0), Point(1, 1)],
        crs=crs,
    )
    expected_gdf.to_parquet(test_file)

    # Act: Read with GeoParquetReader
    reader = GeoParquetReader()
    result_gdf = reader.read(test_file)

    # Assert: CRS is preserved (compare EPSG codes)
    assert result_gdf.crs is not None
    assert result_gdf.crs == expected_gdf.crs
    assert result_gdf.crs.to_epsg() == 3857


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.ErrorHandling")
def test_geoparquet_reader_file_not_found() -> None:
    """GeoParquetReader raises FileNotFoundError for missing file."""
    # Arrange
    nonexistent_file = Path("/nonexistent/path/missing.parquet")
    reader = GeoParquetReader()

    # Act & Assert
    with pytest.raises(FileNotFoundError):
        reader.read(nonexistent_file)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.CrsReprojection")
def test_geoparquet_reader_with_target_crs_reprojects(tmp_path: Path) -> None:
    """GeoParquetReader reprojects to target_crs when specified."""
    # Arrange: Create test GeoParquet file in EPSG:4326 (WGS84)
    test_file = tmp_path / "test_reproject.parquet"
    # Use non-zero coordinates that will change when reprojected
    source_gdf = gpd.GeoDataFrame(
        {"id": [1, 2]},
        geometry=[Point(10, 20), Point(30, 40)],
        crs="EPSG:4326",
    )
    source_gdf.to_parquet(test_file)

    # Act: Read with target_crs=EPSG:3857 (Web Mercator)
    reader = GeoParquetReader(target_crs="EPSG:3857")
    result_gdf = reader.read(test_file)

    # Assert: CRS has been reprojected
    assert result_gdf.crs is not None
    assert result_gdf.crs.to_epsg() == 3857
    # Verify coordinates actually changed (reprojection occurred)
    # 10 degrees longitude in WGS84 != 10 in Web Mercator
    assert result_gdf.geometry[0].x != 10.0
    assert result_gdf.geometry[0].y != 20.0


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.ErrorHandling")
def test_geoparquet_reader_with_invalid_crs_raises_error(tmp_path: Path) -> None:
    """GeoParquetReader raises CRSError when target_crs is invalid."""
    # Arrange: Create test GeoParquet file
    test_file = tmp_path / "test_invalid_crs.parquet"
    source_gdf = gpd.GeoDataFrame(
        {"id": [1]},
        geometry=[Point(0, 0)],
        crs="EPSG:4326",
    )
    source_gdf.to_parquet(test_file)

    # Act & Assert: Invalid CRS should raise CRSError from pyproj
    from pyproj.exceptions import CRSError

    reader = GeoParquetReader(target_crs="INVALID:CRS")
    with pytest.raises(CRSError):
        reader.read(test_file)


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.ColumnFiltering")
def test_geoparquet_reader_with_columns_filter(tmp_path: Path) -> None:
    """GeoParquetReader filters columns when columns parameter specified."""
    # Arrange: Create test GeoParquet file with multiple attribute columns
    test_file = tmp_path / "test_columns.parquet"
    source_gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [10, 20, 30],
            "category": ["x", "y", "z"],
        },
        geometry=[Point(0, 0), Point(1, 1), Point(2, 2)],
        crs="EPSG:4326",
    )
    source_gdf.to_parquet(test_file)

    # Act: Read with columns filter selecting only id and value
    reader = GeoParquetReader(columns=["id", "value"])
    result_gdf = reader.read(test_file)

    # Assert: Only specified columns are present
    assert "id" in result_gdf.columns
    assert "value" in result_gdf.columns
    assert "name" not in result_gdf.columns
    assert "category" not in result_gdf.columns
    assert list(result_gdf["id"]) == [1, 2, 3]
    assert list(result_gdf["value"]) == [10, 20, 30]


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.GeometryPreservation")
def test_geoparquet_reader_columns_preserves_geometry(tmp_path: Path) -> None:
    """GeoParquetReader preserves geometry even when not in columns list."""
    # Arrange: Create test GeoParquet file with multiple attribute columns
    test_file = tmp_path / "test_columns_geom.parquet"
    geometries = [Point(0, 0), Point(1, 1), Point(2, 2)]
    source_gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "value": [10, 20, 30],
        },
        geometry=geometries,
        crs="EPSG:4326",
    )
    source_gdf.to_parquet(test_file)

    # Act: Read with columns filter that does NOT explicitly include geometry
    reader = GeoParquetReader(columns=["name"])
    result_gdf = reader.read(test_file)

    # Assert: Geometry column is preserved even though not in columns list
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    assert result_gdf.geometry.name == "geometry"
    assert "name" in result_gdf.columns
    assert "id" not in result_gdf.columns
    assert "value" not in result_gdf.columns
    # Verify geometry data is correct
    for i, geom in enumerate(result_gdf.geometry):
        assert geom.equals(geometries[i])


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.SpatialFiltering")
def test_geoparquet_reader_with_bbox_filters_data(tmp_path: Path) -> None:
    """GeoParquetReader filters data with bbox parameter."""
    # Arrange: Create test GeoParquet file with points at known locations
    test_file = tmp_path / "test_bbox.parquet"
    source_gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "name": ["origin", "mid", "far"],
        },
        geometry=[Point(0, 0), Point(10, 10), Point(100, 100)],
        crs="EPSG:4326",
    )
    source_gdf.to_parquet(test_file)

    # Act: Read with bbox containing only first two points (minx, miny, maxx, maxy)
    reader = GeoParquetReader(bbox=(-5.0, -5.0, 15.0, 15.0))
    result_gdf = reader.read(test_file)

    # Assert: Only points within bbox are returned
    assert len(result_gdf) == 2
    assert list(result_gdf["id"]) == [1, 2]
    assert list(result_gdf["name"]) == ["origin", "mid"]
    assert result_gdf.geometry[0].equals(Point(0, 0))
    assert result_gdf.geometry[1].equals(Point(10, 10))


@pytest.mark.tier(1)
@pytest.mark.tra("Adapter.Readers.GeoParquetReader.SpatialFiltering")
def test_geoparquet_reader_bbox_returns_empty_when_no_matches(tmp_path: Path) -> None:
    """GeoParquetReader returns empty GeoDataFrame when bbox doesn't intersect any geometries."""
    # Arrange: Create test GeoParquet file with points at known locations
    test_file = tmp_path / "test_bbox_empty.parquet"
    source_gdf = gpd.GeoDataFrame(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
        },
        geometry=[Point(0, 0), Point(10, 10), Point(20, 20)],
        crs="EPSG:4326",
    )
    source_gdf.to_parquet(test_file)

    # Act: Read with bbox that contains no points (far from all geometries)
    reader = GeoParquetReader(bbox=(100.0, 100.0, 200.0, 200.0))
    result_gdf = reader.read(test_file)

    # Assert: Empty GeoDataFrame is returned
    assert isinstance(result_gdf, gpd.GeoDataFrame)
    assert len(result_gdf) == 0
    assert "id" in result_gdf.columns
    assert "name" in result_gdf.columns
    assert result_gdf.geometry.name == "geometry"
