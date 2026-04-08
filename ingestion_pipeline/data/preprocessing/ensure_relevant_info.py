import logging

import xarray

logger = logging.getLogger(__name__)


def ensure_coordinate_existence(ds: xarray.Dataset) -> xarray.Dataset:
    """
    Ensure all dimensions have associated coordinates.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to process.

    Returns
    -------
    xarray.Dataset
        Dataset with coordinates added to dimensions.
    """
    logger.info("Ensuring coordinates exist for all dimensions.")
    for dim in ds.dims:
        if dim not in ds.coords:
            size = ds.sizes[dim]
            if size > 0:
                coords = ds[dim].values
                ds = ds.assign_coords({dim: (dim, coords)})
    return ds
