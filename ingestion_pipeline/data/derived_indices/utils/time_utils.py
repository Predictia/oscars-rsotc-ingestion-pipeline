import xarray
import xarray as xr

from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
    YearPeriod,
)


def get_complete_period_timestamps(
    da: xr.DataArray,
    aggregation: AggregationType,
    valid_fraction: float = 0.8,
    count_nans: bool = True,
) -> xr.DataArray:
    """
    Extract valid timestamps from the first spatial point based on data completeness.

    Automatically selects the first spatial point (lat, lon), evaluates data completeness
    for each aggregation period, and returns the timestamps of periods that meet the completeness threshold.

    Parameters
    ----------
    da : xr.DataArray
        Input DataArray with a time dimension (must have regular frequency).
        Can have multiple spatial or other dimensions (lat, lon, etc.).
        Can be dask-backed or regular numpy array.
    aggregation : AggregationType
        Target aggregation frequency (e.g., MONTHLY, SEASONAL, ANNUAL).
    valid_fraction : float, optional
        Fraction of expected elements required to consider a period valid.
        Must be between 0 and 1. Default is 0.8 (80% of expected).
        E.g., 0.8 means at least 80% of expected elements.
    count_nans : bool, optional
        If True, count all elements including NaNs. If False (default),
        count only non-null (valid) elements.

    Returns
    -------
    xr.DataArray
        Timestamps of periods that meet the completeness threshold.
    """
    if not 0 <= valid_fraction <= 1:
        raise ValueError("valid_fraction must be between 0 and 1")

    # Infer sample frequency from the input DataArray
    # Note: time coordinate is always non-dask, so this works fine
    sample_freq_str = xr.infer_freq(da.time)
    if sample_freq_str is None:
        raise ValueError(
            "Time frequency could not be inferred. Ensure the time index is regular."
        )

    # Get expected element count
    expected_count = aggregation.get_expected_elements(sample_freq_str)
    if expected_count is None:
        raise ValueError(
            f"Could not calculate expected elements for {aggregation} with {sample_freq_str}"
        )

    da_reduced = da.isel(lat=0, lon=0)

    # Resample and count elements
    if count_nans:
        # Count all elements (including NaNs)
        count_per_period = (
            xr.ones_like(da_reduced).resample(time=aggregation.value).sum()
        )
    else:
        # Count only non-null elements
        count_per_period = da_reduced.notnull().resample(time=aggregation.value).sum()

    # Compute the counts to get the mask (very fast - only 1D time dimension)
    count_per_period_computed = count_per_period.compute()

    # Create and return validity mask
    threshold = expected_count * valid_fraction
    valid_mask = count_per_period_computed >= threshold

    return valid_mask.time[valid_mask.values].values


def filter_index_by_completeness(
    reference_da: xr.DataArray,
    target_da: xr.DataArray,
    aggregation: AggregationType,
    valid_fraction: float = 0.8,
    count_nans: bool = True,
) -> xr.DataArray:
    """
    Filter an xclim-computed index to remove periods with incomplete data.

    Evaluates data completeness in the reference DataArray for each aggregation period, then applies this
    completeness check to filter the target index DataArray, removing timestamps
    that don't meet the minimum completeness requirements.

    Parameters
    ----------
    reference_da : xr.DataArray
        DataArray used to determine valid periods. Can be dask-backed or regular.
    target_da : xr.DataArray
        DataArray to be filtered based on reference validity. Can be dask-backed or regular.
    aggregation : AggregationType
        Target aggregation frequency (e.g., MONTHLY, SEASONAL, ANNUAL).
    valid_fraction : float, optional
        Fraction of expected elements required. Default is 0.8 (80%).
    count_nans : bool, optional
        If True, count all elements including NaNs in the reference. Default is False.

    Returns
    -------
    xr.DataArray
        Filtered target DataArray containing only timestamps corresponding to
        valid periods identified in the reference DataArray..
    """
    valid_timestamps = get_complete_period_timestamps(
        reference_da,
        aggregation,
        valid_fraction=valid_fraction,
        count_nans=count_nans,
    )
    filtered_target = target_da.sel(time=target_da.time.isin(valid_timestamps))

    return filtered_target


def get_reference_dataset(
    input_dataset: xarray.Dataset,
    historical_dataset: xarray.Dataset,
    ref_period: YearPeriod,
) -> xarray.Dataset:
    """
    Get the reference dataset for a given period.

    Parameters
    ----------
    input_dataset : xarray.Dataset
        The input dataset.
    historical_dataset : xarray.Dataset or None
        The historical dataset.
    ref_period : YearPeriod
        The reference period.

    Returns
    -------
    xarray.Dataset
        The dataset subset for the reference period.

    Raises
    ------
    RuntimeError
        If the reference period is outside the data provided.
    """
    if historical_dataset is not None:
        dataset_ref_period = historical_dataset.sel(time=ref_period.to_slice())
    else:
        dataset_ref_period = input_dataset.sel(time=ref_period.to_slice())
    if dataset_ref_period.time.size == 0:
        raise RuntimeError("Reference period is outside data provided.")
    return dataset_ref_period


def pandas_offset2time_component(aggregation: str) -> str:
    """
    Convert pandas offset string to time component name.

    Parameters
    ----------
    aggregation : str
        Pandas offset string (e.g., "YS", "QS-DEC", "MS").

    Returns
    -------
    str
        Time component name (e.g., "year", "season", "month").

    Raises
    ------
    NotImplementedError
        If the aggregation is not supported.
    """
    if aggregation == "YS":
        resolution = "year"
    elif aggregation == "QS-DEC":
        resolution = "season"
    elif aggregation == "MS":
        resolution = "month"
    else:
        raise NotImplementedError
    return resolution
