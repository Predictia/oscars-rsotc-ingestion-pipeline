import logging
from pathlib import Path

import xarray as xr

logger = logging.getLogger(__name__)


def load_netcdf(file_path: Path) -> xr.Dataset:
    """
    Load a NetCDF file as a xarray.Dataset.

    This function opens a NetCDF file, logs the process, and returns the dataset.

    Parameters
    ----------
    file_path : pathlib.Path
        The path to the NetCDF file to be loaded.

    Returns
    -------
    xarray.Dataset
        The loaded dataset.
    """
    try:
        logger.info(f"Loading NetCDF file from {file_path}")
        dataset = xr.open_dataset(file_path)
        logger.info(f"Successfully loaded NetCDF file: {file_path}")
        return dataset
    except Exception as e:
        logger.error(f"Failed to load NetCDF file from {file_path}: {e}")
        raise


def write_netcdf(dataset: xr.Dataset, path: Path):
    """
    Save a xarray.Dataset as a netCDF file.

    Each file will be saved with a specific encoding where we can define
    the chunk strategy, the compress level etc.

    Parameters
    ----------
    dataset (xarray.Dataset): data stored by dimension
    path (pathlib.Path): output path to save the data
    """
    encoding_var = dict(
        dtype="float32",
        shuffle=True,
        zlib=True,
        complevel=1,
    )
    encoding = {var: encoding_var for var in dataset.data_vars}
    dataset.to_netcdf(path=path, encoding=encoding, engine="h5netcdf")
    return path
