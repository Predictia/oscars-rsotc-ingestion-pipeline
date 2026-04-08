import numpy as np
import pytest
import xarray as xr

from ingestion_pipeline.data.preprocessing.renaming_dimensions import (
    rename_spatial_dimensions,
)


def create_dataset(dims):
    data = np.random.rand(*(2 for _ in dims))
    coords = {d: np.arange(2) for d in dims}
    return xr.Dataset({"data": (dims, data)}, coords=coords)


def test_rlon_longitude():
    ds = create_dataset(["rlon", "rlat", "longitude", "latitude"])
    renamed = rename_spatial_dimensions(ds)
    assert "x" in renamed.dims
    assert "y" in renamed.dims
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims
    assert "rlon" not in renamed.dims
    assert "longitude" not in renamed.dims


def test_nav_lat():
    ds = create_dataset(["nav_lon", "nav_lat"])
    renamed = rename_spatial_dimensions(ds)
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims
    assert "nav_lat" not in renamed.dims


def test_i_longitude():
    ds = create_dataset(["i", "j", "longitude", "latitude"])
    renamed = rename_spatial_dimensions(ds)
    assert "x" in renamed.dims
    assert "y" in renamed.dims
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims
    assert "i" not in renamed.dims


def test_i_lon():
    ds = create_dataset(["i", "j", "lon", "lat"])
    renamed = rename_spatial_dimensions(ds)
    assert "x" in renamed.dims
    assert "y" in renamed.dims
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims
    assert "i" not in renamed.dims


def test_rlon_only():
    ds = create_dataset(["rlon", "rlat"])
    renamed = rename_spatial_dimensions(ds)
    assert "x" in renamed.dims
    assert "y" in renamed.dims
    assert "rlon" not in renamed.dims


def test_nj_ni():
    ds = create_dataset(["ni", "nj"])
    renamed = rename_spatial_dimensions(ds)
    assert "x" in renamed.dims
    assert "y" in renamed.dims
    assert "ni" not in renamed.dims


def test_lon_already_correct():
    ds = create_dataset(["lon", "lat"])
    renamed = rename_spatial_dimensions(ds)
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims


def test_longitude():
    ds = create_dataset(["longitude", "latitude"])
    renamed = rename_spatial_dimensions(ds)
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims
    assert "longitude" not in renamed.dims


def test_x_y_to_lon_lat():
    ds = create_dataset(["x", "y"])
    renamed = rename_spatial_dimensions(ds)
    assert "lon" in renamed.dims
    assert "lat" in renamed.dims
    assert "x" not in renamed.dims


def test_not_implemented():
    ds = create_dataset(["unknown"])
    with pytest.raises(NotImplementedError):
        rename_spatial_dimensions(ds)
