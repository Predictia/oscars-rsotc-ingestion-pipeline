from typing import Optional

from xarray import Dataset


def chunk_dataset(ds: Dataset, chunks: Optional[dict] = None) -> Dataset:
    """Chunk dataset adapting to its dimensional structure."""
    if chunks is None:
        chunks = _determine_chunks(ds)

    return ds.chunk(chunks)


def _determine_chunks(ds: Dataset) -> dict:
    chunks = {}
    if "region" in ds.dims:
        if len(ds.region.values) <= 400:
            chunks["region"] = 1
        else:
            chunks["region"] = 50

    if "combined" in ds.dims:
        chunks["combined"] = -1

    if "time" in ds.dims:
        chunks["time"] = -1

    if "time_filter" in ds.dims:
        chunks["time_filter"] = 1

    if "lat" in ds.dims and "lon" in ds.dims:
        chunks["time"] = 1000
        chunks["lat"] = 100
        chunks["lon"] = 100

    return chunks
