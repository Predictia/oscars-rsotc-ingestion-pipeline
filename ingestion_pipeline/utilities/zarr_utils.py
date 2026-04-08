import logging
import os

import xarray as xr

logger = logging.getLogger(__name__)


def convert_netcdfs_to_zarr(input_files, output_file, clean_input_files=False):
    """
    Convert a list of NetCDF files into a single Zarr dataset. Supports zipped output.

    Parameters
    ----------
    input_files : list of str
        Paths of the input NetCDF files.
    output_file : str
        Path to save the merged Zarr dataset. Can be a directory or a `.zip` file.
    clean_input_files : bool, optional
        If True, deletes the input NetCDF files after conversion (default is False).

    Returns
    -------
    str
        Path to the Zarr dataset.
    """
    logger.info(f"Converting {len(input_files)} NetCDF files to Zarr at {output_file}")

    try:
        # Open all NetCDF files as a single dataset
        ds = xr.open_mfdataset(input_files, combine="by_coords", parallel=True)

        # Rechunk the dataset to ensure uniform chunk sizes
        ds = ds.chunk({"time": 350, "lat": 50, "lon": 50})

        # Zarr as a directory
        ds.to_zarr(output_file, mode="w", consolidated=True)
        logger.info(f"Successfully created Zarr directory dataset at {output_file}")

        # Clean up input files if requested
        if clean_input_files:
            logger.info("Cleaning up input NetCDF files...")
            for file in input_files:
                try:
                    os.remove(file)
                    logger.info(f"Deleted file: {file}")
                except Exception as e:
                    logger.error(f"Failed to delete file {file}: {e}")

    except Exception as e:
        logger.error(f"Failed to convert NetCDF files to Zarr: {e}")
        raise

    return output_file
