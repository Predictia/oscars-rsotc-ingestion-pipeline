import operator

import xarray
from xclim.core.calendar import percentile_doy
from xsdba.nbutils import quantile

from ingestion_pipeline.data.derived_indices.indices.data_models import (
    ReferencePercentile,
)
from ingestion_pipeline.data.derived_indices.utils.time_utils import (
    get_reference_dataset,
    pandas_offset2time_component,
)


def get_percentile(
    input_dataset: xarray.Dataset,
    historical_dataset: xarray.Dataset,
    varname: str,
    reference_percentile: ReferencePercentile,
    freq: str,
) -> xarray.Dataset:
    """
    Compute the percentile of a variable in a dataset.

    Parameters
    ----------
    input_dataset : xarray.Dataset
        The input dataset containing the variable.
    historical_dataset : xarray.Dataset
        The historical dataset to use as reference.
    varname : str
        The name of the variable to compute the percentile for.
    reference_percentile : ReferencePercentile
        The reference percentile configuration.
    freq : str
        The frequency of the output data.

    Returns
    -------
    xarray.Dataset
        The dataset containing the computed percentile.

    Raises
    ------
    NotImplementedError
        If the reference percentile kind is not supported.
    """
    ref_period = reference_percentile.reference_period

    dataset_ref_period = get_reference_dataset(
        input_dataset, historical_dataset, ref_period
    )

    percentile = reference_percentile.value
    da_ref_period = dataset_ref_period[varname]

    # Apply threshold before computing the percentile, this is mostly for precipitation.
    if reference_percentile.threshold is not None:
        value = reference_percentile.threshold.value
        op = getattr(operator, reference_percentile.operator)
        mask = op(da_ref_period, value)
        da_ref_period = da_ref_period.where(mask)

    def custom_percentile_calculation(group: xarray.DataArray):
        """
        Compute the percentile for a given group.

        Parameters
        ----------
        group : xarray.DataArray
            The data array group.

        Returns
        -------
        xarray.DataArray
            The computed quantile.
        """
        return quantile(group, q=[percentile / 100], dim="time")

    time_component = pandas_offset2time_component(freq)
    # regular is the percentile over time, doy is a percentile for each day of the year,
    # using a moving window.
    if reference_percentile.kind == "regular":
        # If dest_freq is year, then this is simple, else we need to use groupby
        # to compute the percentile for each month or season
        if time_component == "year":
            dataset_percentile = custom_percentile_calculation(
                da_ref_period
            ).to_dataset(name=varname)
        else:
            dataset_percentile = (
                da_ref_period.groupby(f"time.{time_component}")
                .map(custom_percentile_calculation)
                .to_dataset(name=varname)
            )
        dataset_percentile = dataset_percentile.squeeze().drop_vars(
            "quantiles", errors="ignore"
        )
    elif reference_percentile.kind == "doy":
        dataset_percentile = (
            percentile_doy(
                da_ref_period, per=percentile, window=reference_percentile.window
            )
            .to_dataset(name=varname)
            .squeeze()
            .drop_vars("percentiles", errors="ignore")
        )
    else:
        raise NotImplementedError
    return dataset_percentile
