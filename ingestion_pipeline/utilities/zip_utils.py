import logging
import tempfile
import zipfile
from pathlib import Path

import xarray as xr

logger = logging.getLogger(__name__)


def load_zip(zip_path: Path) -> xr.Dataset:
    """
    Load and merge all NetCDF files inside a zip archive into a single xarray.Dataset.

    This function extracts all .nc files from a zip archive into a temporary directory,
    opens them using xarray.open_mfdataset, logs the process, and returns the merged dataset.

    Parameters
    ----------
    zip_path : pathlib.Path
        The path to the zip archive containing NetCDF files.

    Returns
    -------
    xarray.Dataset
        The merged dataset from all NetCDF files in the archive.
    """
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            logger.info(
                f"Extracting zip archive {zip_path} to temporary directory {tmpdir_path}"
            )

            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmpdir_path)

            netcdf_files = sorted(tmpdir_path.rglob("*.nc"))

            if not netcdf_files:
                raise FileNotFoundError(
                    "No NetCDF (.nc) files found inside the zip archive."
                )

            logger.info(
                f"Opening {len(netcdf_files)} NetCDF files from extracted zip content."
            )
            dataset = xr.open_mfdataset(netcdf_files, combine="by_coords")

            logger.info(f"Successfully loaded and merged NetCDF files from {zip_path}")
            return dataset

    except Exception as e:
        logger.error(f"Failed to load NetCDF files from zip archive {zip_path}: {e}")
        raise


def zip_directory(directory_path: Path, zip_path: Path) -> None:
    """
    Zip a directory.

    Parameters
    ----------
    directory_path : pathlib.Path
        The path to the directory to zip.
    zip_path : pathlib.Path
        The path to the output zip file.
    """
    try:
        logger.info(f"Zipping directory {directory_path} to {zip_path}")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zip_ref:
            for file_path in directory_path.rglob("*"):
                if file_path.is_file():
                    zip_ref.write(file_path, file_path.relative_to(directory_path))
        logger.info(f"Successfully zipped directory {directory_path} to {zip_path}")
    except Exception as e:
        logger.error(f"Failed to zip directory {directory_path}: {e}")
        raise
