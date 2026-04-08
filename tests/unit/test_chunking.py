import numpy as np
import xarray as xr

from ingestion_pipeline.utilities.chunking import chunk_dataset


def test_chunk_dataset_defaults():
    # Create simple dataset
    ds = xr.Dataset(
        {"data": (("time", "lat", "lon"), np.random.rand(10, 10, 10))},
        coords={"time": np.arange(10), "lat": np.arange(10), "lon": np.arange(10)},
    )

    # Test default chunking for lat/lon/time
    chunked = chunk_dataset(ds)
    # The function sets chunks time=500, lat=50, lon=50 if they exist
    # Since our dims are smaller, it should just be chunked.
    # We can check if it is now a dask array
    assert chunked["data"].chunks is not None


def test_chunk_dataset_region():
    # Test small region
    ds = xr.Dataset(
        {"data": (("region",), np.random.rand(100))}, coords={"region": np.arange(100)}
    )
    chunked = chunk_dataset(ds)
    # logic: if len <= 400, chunks["region"] = 1
    assert chunked.chunks["region"][0] == 1

    # Test large region
    ds_large = xr.Dataset(
        {"data": (("region",), np.random.rand(500))}, coords={"region": np.arange(500)}
    )
    chunked_large = chunk_dataset(ds_large)
    # logic: else chunks["region"] = 50
    assert chunked_large.chunks["region"][0] == 50


def test_chunk_dataset_combined():
    ds = xr.Dataset(
        {"data": (("combined",), np.random.rand(10))},
        coords={"combined": np.arange(10)},
    )
    chunked = chunk_dataset(ds)
    # logic: chunks["combined"] = -1 (one chunk)
    assert len(chunked.chunks["combined"]) == 1


def test_chunk_dataset_custom():
    ds = xr.Dataset({"data": (("x",), np.arange(10))})
    input_chunks = {"x": 2}
    chunked = chunk_dataset(ds, chunks=input_chunks)
    assert chunked.chunks["x"][0] == 2
