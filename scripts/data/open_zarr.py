"""
Module to open and inspect Zarr datasets from S3 or local storage.
"""

from typing import Any

import xarray as xr

from ingestion_pipeline.utilities.s3_handlers import S3Config, S3Handler


def open_zarr_dataset(
    zarr_path: str,
    variable: str | None = None,
    chunks: dict[str, Any] | None = None,
) -> xr.Dataset:
    """
    Open a Zarr dataset from either local storage or an S3 remote location.

    This function infers if the provided path is an S3 URI or a local path
    and opens it accordingly, optionally returning a specific variable.

    Parameters
    ----------
    zarr_path : str
        Path to the Zarr dataset (e.g., "s3://bucket/data.zarr" or "/local/data.zarr").
    variable : str | None
        Specific variable name to extract from the dataset.
    chunks : dict[str, Any] | None
        Dask chunking dictionary. Default is None.

    Returns
    -------
    xr.Dataset
        The loaded Xarray dataset.
    """
    if zarr_path.startswith("s3://"):
        s3_config = S3Config.from_env()
        s3_handler = S3Handler(s3_config=s3_config)
        
        ds = s3_handler.read_file(
            zarr_path=zarr_path,
            variable=variable,
            chunks=chunks,
        )
    else:
        ds = xr.open_zarr(zarr_path, consolidated=True, chunks=chunks or {})
        
        if variable:
            if variable not in ds:
                raise ValueError(
                    f"Variable '{variable}' not found in dataset. "
                    f"Available variables: {list(ds.data_vars)}"
                )
            ds = ds[[variable]]

    return ds


if __name__ == "__main__":
    zarr_path = "s3://oscars/tas_None_ERA5_gridded.zarr"
    variable = "tas"

    dataset = open_zarr_dataset(
        zarr_path=zarr_path,
        variable=variable,
        chunks=None
    )
    
    print("Dataset Summary:")
    print("----------------")
    print(dataset)
