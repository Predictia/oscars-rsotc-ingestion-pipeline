import operator
import os
from collections.abc import Callable
from typing import Any

import xarray

from ingestion_pipeline.data.derived_indices.indices.data_models import (
    Index,
    ReferencePercentile,
)
from ingestion_pipeline.data.derived_indices.indices.time_models import (
    SeasonLimits,
)
from ingestion_pipeline.data.derived_indices.utils.percentile import (
    get_percentile,
)
from ingestion_pipeline.data.derived_indices.utils.time_utils import (
    pandas_offset2time_component,
)
from ingestion_pipeline.data.derived_indices.utils.wrappers import (
    check_minimum_values,
)


def compute_index(
    index: Index,
    input_dataset: xarray.Dataset,
    historical_dataset: xarray.Dataset | None = None,
    dest_freq: str = "MS",
    orig_freq: str = "D",
) -> xarray.Dataset:
    """
    Compute climate indices in a flexible way.

    Parameters
    ----------
    index : Index
        Index object that determines how to calculate a climate index.
    input_dataset : xarray.Dataset
        Input data to compute the index. Must contain the variables defined in the
        vars2use attribute of the index object. Regular (time, lat, lon) grids are
        currently supported. Projected grids will work most of the time, but have not
        been tested. Time resolution must be daily.
    historical_dataset : xarray.Dataset or None, optional
        Same as input_dataset, but for the historical period. Needed to calculate
        reference climatologies for climate projections. If None, input_dataset will be
        used. Time resolution must be daily. Default is None.
    dest_freq : str, optional
        Frequency of the output data. One of "MS", "Q-DEC" or "YS". Default is "MS".
    orig_freq : str, optional
        Frequency of the input data. One of "D", "MS", "Q-DEC" or "YS". Default is "D".

    Returns
    -------
    xarray.Dataset
        Dataset with the index data. The variable name is the short name defined in
        the index object.
    """
    # Get the main input variable name, in case there are many, it is the first one.
    varname = _get_main_variable_name(index)
    # Check dest_freq is allowed for this index
    _validate_dest_freq(dest_freq, index)
    # Check if the input variables are present with the right units
    _check_variables_and_units(input_dataset, historical_dataset, index)
    # By default the index is just a copy of the input. The simplest indices are just
    # aggregations like monthly temperature
    dataset_index = input_dataset.copy()

    if historical_dataset is None:
        historical_data = dataset_index.copy()
    else:
        historical_data = historical_dataset

    if index.reference_percentile is not None:
        dataset_percentile = get_percentile(
            dataset_index,
            historical_data,
            varname,
            index.reference_percentile,
            dest_freq,
        )
    else:
        dataset_percentile = None

    if index.direct_function is not None:
        # xclim does already resample/aggregate in time so we follow a different path
        dataset_index = _compute_direct_function(
            dataset_index,
            historical_data,
            dest_freq,
            index.direct_function,
            index.direct_function_kwargs,
            index.direct_function_signature,
            percentile=dataset_percentile,
        )
    # Use the Index attributes to compute the index.
    else:
        # Select a part of the year
        if index.season_limits is not None:
            dataset_index = select_season_limits(dataset_index, index.season_limits)
        # Compare with a constant threshold
        if index.threshold is not None:
            op = getattr(operator, index.threshold.operator)
            dataset_index = op(dataset_index, index.threshold.value)
            # Set the threshold in the attributes
            dataset_index.coords["threshold"] = float(index.threshold.value)
        # Compare with the reference percentile
        elif index.needs_reference_percentile:
            if dataset_percentile is None or index.reference_percentile is None:
                raise RuntimeError("Reference percentile needed but not computed.")
            dataset_index = compare_with_ref_percentile(
                dataset_index, dataset_percentile, dest_freq, index.reference_percentile
            )
        # Resample to the dest_freq
        if index.agg_function is None:
            raise RuntimeError(
                "If no direct direct_function is provided, an agg_function must be provided."
            )
        else:
            dataset_index = resample_dataset(
                dataset_index,
                dest_freq,
                index.agg_function,
                orig_freq,
                False if index.season_limits is not None else True,
            )
    # Rename to the Index.short_name
    if index.short_name not in dataset_index:
        dataset_index = dataset_index.rename(
            {index.vars2use[0].short_name: index.short_name}
        )
    # Set attributes
    dataset_index[index.short_name].attrs["long_name"] = index.long_name
    dataset_index[index.short_name].attrs["units"] = index.units
    # Apply nan mask, as some operations as sum do not propagate them automatically
    nan_mask = ~input_dataset[varname].isnull().all(dim="time")
    dataset_index = dataset_index.astype("float32").where(nan_mask)
    return dataset_index


def _check_variables_and_units(
    input_dataset: xarray.Dataset,
    historical_dataset: xarray.Dataset | None,
    index: Index,
):
    """
    Check if the variables are present with the right units.

    Parameters
    ----------
    input_dataset : xarray.Dataset
        The input dataset to check.
    historical_dataset : xarray.Dataset or None
        The historical dataset to check.
    index : Index
        The index object containing the required variables and units.

    Raises
    ------
    RuntimeError
        If a variable is missing or has wrong units.
    """
    datasets_to_check = [
        input_dataset,
    ]
    if historical_dataset is not None:
        datasets_to_check.append(historical_dataset)
    for variable in index.vars2use:
        for dataset_to_check in datasets_to_check:
            if variable.name not in dataset_to_check:
                raise RuntimeError(f"Input variable {variable.name} not found.")
            if os.environ.get("XRINDICES_CHECK_UNITS", 0):
                units_in_input = dataset_to_check[variable.name].attrs["units"]
                if units_in_input != variable.units:
                    raise RuntimeError(
                        f"Wrong units found for variable {variable.name}: expected "
                        f"{variable.units} but found {units_in_input}"
                    )


def compare_with_ref_percentile(
    input_dataset: xarray.Dataset,
    dataset_percentile: xarray.Dataset,
    dest_freq: str,
    reference_percentile: ReferencePercentile,
) -> xarray.Dataset:
    """
    Compare the input dataset with a reference percentile.

    Parameters
    ----------
    input_dataset : xarray.Dataset
        The input dataset to compare.
    dataset_percentile : xarray.Dataset
        The dataset containing the percentile values.
    dest_freq : str
        The destination frequency string (e.g., "MS", "YS").
    reference_percentile : ReferencePercentile
        The reference percentile object defining the operator.

    Returns
    -------
    xarray.Dataset
        The result of the comparison.
    """
    time_component = pandas_offset2time_component(dest_freq)
    op = getattr(operator, reference_percentile.operator)
    # We can compare groupby object with datasets that have a coordinate with
    # the same label as the groups (month or season in this case)
    # https://docs.xarray.dev/en/stable/user-guide/groupby.html#grouped-arithmetic
    dataset_to_compare: Any
    if time_component == "year":
        # In this case there is no groupby needed
        dataset_to_compare = input_dataset
    else:
        dataset_to_compare = input_dataset.groupby(f"time.{time_component}")
    dataset_index = op(dataset_to_compare, dataset_percentile)
    return dataset_index


def resample_dataset(
    dataset_index: xarray.Dataset,
    dest_freq: str,
    agg_function: Callable,
    orig_freq: str | None = None,
    apply_min_values: bool = True,
) -> xarray.Dataset:
    """
    Resample the dataset to the destination frequency using the aggregation function.

    Parameters
    ----------
    dataset_index : xarray.Dataset
        The dataset to resample.
    dest_freq : str
        The destination frequency.
    agg_function : Callable
        The aggregation function to use.
    orig_freq : str or None
        The original frequency of the data.
    apply_min_values : bool, optional
        Whether to check for minimum values requirements. Default is True.

    Returns
    -------
    xarray.Dataset
        The resampled dataset.
    """
    if apply_min_values:
        if orig_freq is None:
            raise ValueError("orig_freq must be provided if apply_min_values is True")
        agg_function_wrapped = check_minimum_values(
            agg_function, freq=dest_freq, orig_freq=orig_freq
        )
    else:
        agg_function_wrapped = agg_function
    dataset_index = dataset_index.resample(
        time=dest_freq, label="left", closed="left"
    ).map(agg_function_wrapped, shortcut=True)
    return dataset_index


def _compute_direct_function(
    dataset_index: xarray.Dataset,
    historical_dataset: xarray.Dataset,
    dest_freq: str,
    direct_function: Callable,
    direct_function_kwargs: dict,
    direct_function_signature: str,
    percentile: xarray.Dataset | None = None,
) -> xarray.Dataset:
    """
    Compute the index using a direct function.

    Parameters
    ----------
    dataset_index : xarray.Dataset
        The input dataset.
    historical_dataset : xarray.Dataset
        The historical dataset.
    dest_freq : str
        The destination frequency.
    direct_function : Callable
        The function to compute the index.
    direct_function_kwargs : dict
        Keyword arguments for the direct function.
    direct_function_signature : str
        The signature type of the direct function.
    percentile : xarray.Dataset or None, optional
        The percentile dataset, if required. Default is None.

    Returns
    -------
    xarray.Dataset
        The computed index dataset.

    Raises
    ------
    RuntimeError
        If the signature is unknown or required arguments are missing.
    """
    # Direct function can get the reference percentile if they needed, or the historical
    # data.

    match direct_function_signature:
        case "historical":
            args = (dataset_index, historical_dataset)
        case "percentile":
            if percentile is None:
                raise RuntimeError(
                    "Need to provide a reference percentile for this signature."
                )
            args = (dataset_index, percentile)
        case "single_dataset":
            args = (dataset_index,)  # type: ignore
        case _:
            raise RuntimeError(f"Unkown signature {direct_function_signature}")

    dataset_index = direct_function(
        *args,
        freq=dest_freq,
        **direct_function_kwargs,
    )
    return dataset_index


def _validate_dest_freq(dest_freq: str, index: Index) -> None:
    """
    Validate if the destination frequency is allowed for the index.

    Parameters
    ----------
    dest_freq : str
        The destination frequency to check.
    index : Index
        The index object containing valid frequencies.

    Raises
    ------
    RuntimeError
        If the destination frequency is not valid for the index.
    """
    if dest_freq not in index.valid_dest_freq:
        raise RuntimeError(f"Index {index} cannot be computed for {dest_freq=}")


def _get_main_variable_name(index: Index) -> str:
    """
    Get the main variable name from the index.

    Parameters
    ----------
    index : Index
        The index object.

    Returns
    -------
    str
        The short name of the variable.
    """
    # For both single and multivariable indices, we use the first one as reference
    # for some checks.
    return index.vars2use[0].short_name


def select_season_limits(
    dataset: xarray.Dataset, seas_limits: SeasonLimits
) -> xarray.Dataset:
    """
    Select a specific time period within a year from a dataset.

    This function filters the data projections based on a time period defined
    by the start and end parameters in `index_params.period`.

    Parameters
    ----------
    dataset : xarray.Dataset
        The dataset of projections containing a time dimension.

    seas_limits : SeasonLimits
        The season limits object defining start and end of the season.

    Returns
    -------
    xarray.Dataset
        The dataset filtered by the season limits.
    """
    # Convert the time dimension to a Pandas DatetimeIndex for easier manipulation
    months = dataset.time.dt.month
    days = dataset.time.dt.day

    # Create a mask to filter the dataset based on the specified period
    mask = (
        (months == seas_limits.start.month) & (days >= seas_limits.start.day)
        | (months > seas_limits.start.month) & (months < seas_limits.end.month)
        | (months == seas_limits.end.month) & (days <= seas_limits.end.day)
    )
    # Apply the mask to filter the dataset
    return dataset.isel(time=mask)
