from pathlib import Path
from unittest.mock import MagicMock, patch

import geopandas as gpd
import numpy as np
import pytest
import xarray as xr
from shapely.geometry import Polygon

from ingestion_pipeline.utilities.spatial_agg import (
    SpatialAggregation,
    aggregate_regions,
)


# Create a dummy dataset
@pytest.fixture
def dummy_dataset():
    lat = np.linspace(0, 10, 5)
    lon = np.linspace(0, 10, 5)
    data = np.random.rand(5, 5, 2)
    ds = xr.Dataset(
        {"temp": (("lat", "lon", "time"), data)},
        coords={"lat": lat, "lon": lon, "time": [0, 1]},
    )
    return ds


# Create a dummy GeoDataFrame
@pytest.fixture
def dummy_gdf():
    poly1 = Polygon([(0, 0), (0, 5), (5, 5), (5, 0)])
    poly2 = Polygon([(5, 5), (5, 10), (10, 10), (10, 5)])
    gdf = gpd.GeoDataFrame(
        {
            "NAME_LATN": ["Region1", "Region2"],
            "CNTR_CODE": ["R1", "R2"],
            "NUTS_ID": ["N1", "N2"],
            "geometry": [poly1, poly2],
        }
    )
    return gdf


@patch("ingestion_pipeline.utilities.spatial_agg.geopandas.read_file")
@patch("ingestion_pipeline.utilities.spatial_agg.regionmask.from_geopandas")
def test_spatial_aggregation_compute(
    mock_from_geopandas, mock_read_file, dummy_dataset, dummy_gdf
):
    mock_read_file.return_value = dummy_gdf

    # Mock regionmask result
    mock_regions = MagicMock()
    # Mask with shape (region, lat, lon)
    # 2 regions, 5x5 grid
    mock_mask = xr.DataArray(
        np.random.randint(0, 2, (2, 5, 5)),
        coords={
            "region": [0, 1],
            "lat": dummy_dataset.lat,
            "lon": dummy_dataset.lon,
            "names": ("region", ["Region1", "Region2"]),
            "abbrevs": ("region", ["N1", "N2"]),
        },
        dims=("region", "lat", "lon"),
    )
    # Also add names to the mask
    mock_mask.coords["names"] = ("region", ["Region1", "Region2"])
    mock_mask.coords["abbrevs"] = ("region", ["N1", "N2"])
    mock_regions.mask_3D.return_value = mock_mask
    mock_regions.abbrevs = ["N1", "N2"]
    mock_regions.__len__.return_value = 2
    mock_from_geopandas.return_value = mock_regions

    agg = SpatialAggregation(dummy_dataset, "dummy_path.geojson")
    result = agg.compute()

    assert "region" in result.dims
    assert result.sizes["region"] == 2
    # Verify valid data
    assert result["temp"].notnull().all()


@patch("ingestion_pipeline.utilities.spatial_agg.importlib.resources.files")
@patch("ingestion_pipeline.utilities.spatial_agg.SpatialAggregation")
def test_aggregate_regions(mock_sa_cls, mock_resources, dummy_dataset):
    mock_sa_instance = mock_sa_cls.return_value
    mock_sa_instance.compute.return_value = dummy_dataset  # Return same for simplicity

    mock_resources.return_value = Path("/mock/path")

    result = aggregate_regions(dummy_dataset, "NUTS-0")

    assert result == dummy_dataset
    mock_sa_cls.assert_called_once()
