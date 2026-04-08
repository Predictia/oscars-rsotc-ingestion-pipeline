import logging

import xarray as xr

logger = logging.getLogger(__name__)


def rename_variable_names(ds: xr.Dataset, variable_name: str) -> xr.Dataset:
    """
    Rename variables in the dataset based on the provided parameter information.

    Parameters
    ----------
    ds : xarray.Dataset
        The input dataset containing variables to rename.
    variable_name : str
        The name of the variable to rename.

    Returns
    -------
    xarray.Dataset
        A dataset with renamed variables.
    """
    logger.info("Renaming variables in the dataset based on parameter information.")

    assert len(ds.data_vars) == 1
    var_in_ds = list(ds.data_vars)[0]

    ds = ds.rename({var_in_ds: variable_name})
    logger.info("Variable renaming completed.")
    return ds
