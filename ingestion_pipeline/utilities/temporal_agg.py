from calendar import monthrange
from dataclasses import dataclass
from typing import Tuple

import numpy
import xarray

from ingestion_pipeline.utilities.constants import mapping_months, mapping_statistics


@dataclass
class TemporalAggregation:
    """
    A class for computing time aggregation for a given period range and season.

    Parameters
    ----------
    dataset : xarray.Dataset
        The input dataset containing the data to be aggregated.
    statistical : str
        The statistical function to apply.
    period_range : str
        The period range to compute the climatology for,
        in the format 'start_year-end_year'.
    time_filter : str
        The season to aggregate over, in the format 'start_month-end_month'.
    """

    dataset: xarray.Dataset
    statistical: str
    time_filter: str
    product_type: str
    period_range: str = "all"

    def sel_time_filter(self):
        """
        Apply the time filter to the time series dataset.

        Returns
        -------
        xarray.Dataset.
        """
        # Parse the season string into start and end months
        start_month, end_month = self.time_filter.split("-")

        # Select the temporal range of interest
        if self.period_range == "all":
            period_range_start = numpy.datetime_as_string(
                self.dataset.time.values[0], unit="Y"
            )
            period_range_end = numpy.datetime_as_string(
                self.dataset.time.values[-1], unit="Y"
            )
        else:
            period_range_start, period_range_end = self.period_range.split("-")

        data_for_seasons = []

        for year in range(int(period_range_start), int(period_range_end) + 1):
            # Select the data for the current year and season
            if int(start_month) <= int(end_month):
                num_days = monthrange(year, int(end_month))
                season_range = slice(
                    f"{year}-{start_month}-01", f"{year}-{end_month}-{num_days[1]}"
                )
            else:
                num_days = monthrange(year, int(end_month))
                season_range = slice(
                    f"{year - 1}-{start_month}-01", f"{year}-{end_month}-{num_days[1]}"
                )

            data_for_season = self.dataset.sel(time=season_range)
            data_for_seasons.append(data_for_season)

        data_for_period = xarray.concat(data_for_seasons, dim="time")
        return data_for_period

    def compute(self) -> Tuple[xarray.Dataset, xarray.Dataset]:
        """
        Compute the climatology for the specified period range.

        Returns
        -------
        data_for_period: xarray.Dataset
            The data for the period range applied the time_filter
        time_agg_product : xarray.Dataset
            The climatology  or time series for the specified period range.
        """
        data_for_period = self.sel_time_filter()
        # Select the statistical function to compute climatology
        statistical_function = mapping_statistics[self.statistical]
        start_month = int(self.time_filter.split("-")[0])
        end_month = int(self.time_filter.split("-")[-1])

        time_agg_product = data_for_period.resample(
            time=f"YS-{mapping_months[start_month]}"
        ).reduce(statistical_function, "time")

        if start_month > end_month:
            time_agg_product["time"] = (
                time_agg_product["time"].values.astype("datetime64[Y]")
                + numpy.timedelta64(1, "Y")
            ).astype("datetime64[ns]")

        if self.product_type == "climatology":
            time_agg_product = time_agg_product.mean("time")
        elif self.product_type == "temporal_series":
            time_agg_product = time_agg_product.resample(time="YS").reduce(
                mapping_statistics["mean"], "time"
            )
        else:
            raise NotImplementedError

        return data_for_period, time_agg_product
