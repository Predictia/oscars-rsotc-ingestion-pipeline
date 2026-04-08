"""
Custom index calculations for derived indices.

Implements specialized index computation functions that extend xclim and xrindices
functionality.
"""

from typing import Any

import xarray as xr
from xclim.indices import fraction_over_precip_thresh


def fraction_over_thresh_p(
    ds_index: xr.Dataset,
    ds_percentile: xr.Dataset,
    **kwargs: Any,
) -> xr.Dataset:
    """
    Compute the fraction of precipitation above a given percentile threshold.

    This function calculates the fraction of total precipitation occurring
    on days when daily precipitation exceeds a specified percentile of the
    reference period.

    Parameters
    ----------
    ds_index : xr.Dataset
        Precipitation dataset for the period to compute the index.
        Must contain a single variable.
    ds_percentile : xr.Dataset
        Reference period dataset used to compute the percentile threshold.
        Must contain the same variable as ds_index.
    **kwargs : dict
        Keyword arguments passed to xclim.indices.fraction_over_precip_thresh,
        such as:
        - thresh : str, optional
            Precipitation threshold (default "1 mm/day").
        - freq : str, optional
            Resampling frequency (default "YS" for annual).

    Returns
    -------
    xr.Dataset
        Computed index as a Dataset with the same variable name as input.

    Raises
    ------
    ValueError
        If ds_index contains more than one variable.

    Notes
    -----
    This is typically used for the "r95ptot" index which represents
    the fraction of annual precipitation from days exceeding the 95th percentile.
    """
    # Extract and validate variable
    list_variables = list(ds_index.data_vars)
    if len(list_variables) != 1:
        raise ValueError(
            f"Expected single variable in ds_index, got {len(list_variables)}. "
            f"Available variables: {list_variables}"
        )

    varname = list_variables[0]
    da = ds_index[varname]

    # Prepare kwargs with percentile data
    computation_kwargs = dict(kwargs)
    computation_kwargs["pr_per"] = ds_percentile[varname]

    # Compute and return as Dataset
    result = fraction_over_precip_thresh(pr=da, **computation_kwargs)
    return result.to_dataset(name=varname)
