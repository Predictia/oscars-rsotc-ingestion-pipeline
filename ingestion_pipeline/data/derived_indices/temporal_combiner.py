import gc
import logging

import numpy as np
import pandas as pd
import xarray as xr

from ingestion_pipeline.data.derived_indices.indices.data_models import Index
from ingestion_pipeline.data.derived_indices.indices.time_models import (
    AggregationType,
    MonthRange,
)

logger = logging.getLogger(__name__)

# Follow the naming convention from ingestion_pipeline.data.constants.time_filters
# Individual months
MONTHS = [
    MonthRange("Jan", 1, 1),
    MonthRange("Feb", 2, 2),
    MonthRange("Mar", 3, 3),
    MonthRange("Apr", 4, 4),
    MonthRange("May", 5, 5),
    MonthRange("Jun", 6, 6),
    MonthRange("Jul", 7, 7),
    MonthRange("Aug", 8, 8),
    MonthRange("Sep", 9, 9),
    MonthRange("Oct", 10, 10),
    MonthRange("Nov", 11, 11),
    MonthRange("Dec", 12, 12),
]

# Multi-month ranges
SEASONS = [
    MonthRange("DecFeb", 12, 2),
    MonthRange("MarMay", 3, 5),
    MonthRange("JunAug", 6, 8),
    MonthRange("SepNov", 9, 11),
]

ANNUAL = MonthRange("Annual", 1, 12)

TIME_FILTER = [n.name for n in MONTHS + SEASONS + [ANNUAL]]


class TimeReferenceCalculator:
    """Calculate time references for different aggregation types."""

    @staticmethod
    def calculate_time_ref(
        timestamp: pd.Timestamp, agg_type: AggregationType
    ) -> pd.Timestamp:
        """
        Calculate the reference time for a given timestamp and aggregation type.

        Parameters
        ----------
        timestamp : pd.Timestamp
            The original timestamp.
        agg_type : AggregationType
            The aggregation type (MONTHLY, SEASONAL, ANNUAL).

        Returns
        -------
        pd.Timestamp
            The calculated reference time.
        """
        match agg_type:
            case AggregationType.MONTHLY:
                return TimeReferenceCalculator.monthly_time_ref(timestamp)
            case AggregationType.SEASONAL:
                return TimeReferenceCalculator.seasonal_time_ref(timestamp)
            case AggregationType.ANNUAL:
                return TimeReferenceCalculator.annual_time_ref(timestamp)
            case _:
                raise ValueError(f"Unsupported aggregation type: {agg_type}")

    @staticmethod
    def reverse_time_ref(
        time_ref: pd.Timestamp,
        agg_type: AggregationType,
    ) -> pd.Timestamp:
        """
        Reverse the time reference calculation to get the original timestamp.

        Parameters
        ----------
        time_ref : pd.Timestamp
            The reference timestamp (as stored in the dataset).
        agg_type : AggregationType
            The aggregation type (MONTHLY, SEASONAL, ANNUAL).

        Returns
        -------
        pd.Timestamp
            The original timestamp before time reference calculation.
        """
        if agg_type == AggregationType.MONTHLY:
            return TimeReferenceCalculator.reverse_monthly_time_ref(time_ref)
        elif agg_type == AggregationType.SEASONAL:
            return TimeReferenceCalculator.reverse_seasonal_time_ref(time_ref)
        elif agg_type == AggregationType.ANNUAL:
            return TimeReferenceCalculator.reverse_annual_time_ref(time_ref)
        else:
            raise ValueError(f"Unsupported aggregation type: {agg_type}")

    @staticmethod
    def monthly_time_ref(timestamp: pd.Timestamp) -> pd.Timestamp:
        """
        Calculate time reference for monthly data.

        Parameters
        ----------
        timestamp : pd.Timestamp
            The original timestamp.

        Returns
        -------
        pd.Timestamp
            The monthly time reference.
        """
        return timestamp

    @staticmethod
    def reverse_monthly_time_ref(time_ref: pd.Timestamp) -> pd.Timestamp:
        """
        Reverse monthly time reference calculation.

        Parameters
        ----------
        time_ref : pd.Timestamp
            The monthly reference timestamp.

        Returns
        -------
        pd.Timestamp
            The original timestamp.
        """
        return time_ref

    @staticmethod
    def seasonal_time_ref(timestamp: pd.Timestamp) -> pd.Timestamp:
        """
        Calculate time reference for seasonal data.

        Special handling for DecFeb (DJF) season:
        - Starts in December but year reference is January of next year.

        Parameters
        ----------
        timestamp : pd.Timestamp
            The original timestamp.

        Returns
        -------
        pd.Timestamp
            The seasonal time reference.
        """
        # DecFeb (DJF) starts in December but year reference is January of next year
        if timestamp.month == 12:
            return pd.Timestamp(year=timestamp.year + 1, month=1, day=1)
        else:
            return timestamp

    @staticmethod
    def reverse_seasonal_time_ref(time_ref: pd.Timestamp) -> pd.Timestamp:
        """
        Reverse seasonal time reference calculation.

        Parameters
        ----------
        time_ref : pd.Timestamp
            The seasonal reference timestamp.

        Returns
        -------
        pd.Timestamp
            The original timestamp.
        """
        if time_ref.month == 1:
            return pd.Timestamp(year=time_ref.year - 1, month=12, day=1)
        else:
            # For other months, time_ref is the same as original
            return time_ref

    @staticmethod
    def annual_time_ref(timestamp: pd.Timestamp) -> pd.Timestamp:
        """
        Calculate time reference for annual data.

        Parameters
        ----------
        timestamp : pd.Timestamp
            The original timestamp.

        Returns
        -------
        pd.Timestamp
            The annual time reference.
        """
        return timestamp

    @staticmethod
    def reverse_annual_time_ref(time_ref: pd.Timestamp) -> pd.Timestamp:
        """
        Reverse annual time reference calculation.

        Parameters
        ----------
        time_ref : pd.Timestamp
            The annual reference timestamp.

        Returns
        -------
        pd.Timestamp
            The original timestamp.
        """
        # For annual, no transformation is applied
        return time_ref


class DatasetCombiner:
    """Combines datasets with different temporal frequencies."""

    def __init__(self, xindice: Index) -> None:
        """
        Initialize the DatasetCombiner.

        Parameters
        ----------
        xindice : Index
            The index definition used for processing.
        """
        self.xindice = xindice

    @property
    def var_name(self) -> str:
        """
        Get the short name of the index variable.

        Returns
        -------
        str
            The variable short name.
        """
        return self.xindice.short_name

    def _process_monthly(self, ds: xr.Dataset) -> list[tuple]:
        """
        Process monthly dataset.

        Parameters
        ----------
        ds : xr.Dataset
            The monthly dataset to process.

        Returns
        -------
        list[tuple]
            A list of tuples (DataArray, period_name, time_ref).
        """
        times = pd.DatetimeIndex(ds.time.values)

        return [
            (
                ds[self.var_name].isel(time=i),
                MonthRange.name_from_month(month_idx, MONTHS),
                TimeReferenceCalculator.monthly_time_ref(times[i]),
            )
            for i, month_idx in enumerate(times.month)
        ]

    def _process_seasonal(self, ds: xr.Dataset) -> list[tuple]:
        """
        Process seasonal dataset.

        Parameters
        ----------
        ds : xr.Dataset
            The seasonal dataset to process.

        Returns
        -------
        list[tuple]
            A list of tuples (DataArray, period_name, time_ref).
        """
        times = pd.DatetimeIndex(ds.time.values)

        result = []
        for i, month_idx in enumerate(times.month.values):
            season = MonthRange.name_from_month(month_idx, SEASONS)

            # DecFeb (DJF) starts in December but year reference is January of next year
            time_ref = TimeReferenceCalculator.seasonal_time_ref(times[i])

            result.append((ds[self.var_name].isel(time=i), season, time_ref))

        return result

    def _process_yearly(self, ds: xr.Dataset) -> list[tuple]:
        """
        Process yearly dataset.

        Parameters
        ----------
        ds : xr.Dataset
            The yearly dataset to process.

        Returns
        -------
        list[tuple]
            A list of tuples (DataArray, period_name, time_ref).
        """
        times = pd.DatetimeIndex(ds.time.values)
        years = times.year.values
        # MonthRange.name_from_month(times[i].month, ANNUAL)
        return [
            (
                ds[self.var_name].isel(time=i),
                ANNUAL.name,
                TimeReferenceCalculator.annual_time_ref(times[i]),
            )
            for i, year in enumerate(years)
        ]

    def combine(
        self, datasets: dict[AggregationType, xr.Dataset], multi_index: bool = False
    ) -> xr.Dataset:
        """
        Combine datasets with different frequencies into a single dataset.

        Parameters
        ----------
        datasets : dict[AggregationType, xr.Dataset]
            Dictionary mapping AggregationType to xarray.Dataset.
        multi_index : bool, optional
            Whether to return a multi-index dataset, by default False.

        Returns
        -------
        xr.Dataset
            Combined dataset with 'time_filter' dimension.
        """
        # Map AggregationType to the corresponding processor method
        processor_map = {
            AggregationType.MONTHLY: self._process_monthly,
            AggregationType.SEASONAL: self._process_seasonal,
            AggregationType.ANNUAL: self._process_yearly,
            # Add other frequencies if needed
        }

        all_records = []
        freq_used = []
        latest_timestamps = {}  # Store the latest timestamp for each aggregation type
        for freq_type, processor in processor_map.items():
            ds = datasets.get(freq_type)
            if ds is None:
                logger.info(f"Skipping {freq_type.name.lower()}, not implemented...")
            else:
                logger.debug(f"Processing {freq_type.name.lower()}...")
                freq_used.append(freq_type)
                latest_timestamps[freq_type] = pd.Timestamp(ds.time.max().values)
                all_records.extend(processor(ds))

        if not all_records:
            if len(datasets) == 1:
                return next(iter(datasets.values()))
            raise ValueError("No datasets provided for combining.")

        # Unpack records and combine
        data_arrays, time_filters, times = zip(*all_records)
        combined_data = xr.concat(data_arrays, dim="combined")

        # Pick one of the datasets for lat/lon coords (assuming all share same grid)
        sample_ds: xr.Dataset = next(iter(datasets.values()))
        del datasets
        gc.collect()

        # Build combined dataset
        times_array = pd.DatetimeIndex(times).values  # Creates datetime64[ns] array
        filters_array = np.array(time_filters, dtype=str)
        ds_combined = xr.Dataset(
            {self.var_name: (["combined", "lat", "lon"], combined_data.values)},
            coords={
                "time": ("combined", times_array),
                "time_filter": ("combined", filters_array),
                "lat": sample_ds.lat,
                "lon": sample_ds.lon,
            },
        )

        if multi_index:
            # OP 1 : Set multiindex and chunking
            ds_combined = ds_combined.set_index(combined=["time", "time_filter"])
        else:
            # OP 2: Imply to delete Nans in time dimension after .sel or .isel
            ds_combined = ds_combined.set_index(
                combined=["time", "time_filter"]
            ).unstack("combined")
            ds_combined = ds_combined.transpose("time", "time_filter", "lat", "lon")

        self._add_attributes(ds_combined, freq_used, latest_timestamps)

        logger.info(
            f"Combined dataset: {len(all_records)} periods, dims={dict(ds_combined.dims)}"
        )
        return ds_combined

    def _add_attributes(
        self,
        ds: xr.Dataset,
        freq_used: list[AggregationType],
        latest_timestamps: dict[AggregationType, pd.Timestamp],
    ) -> None:
        """
        Add metadata attributes to dataset.

        Parameters
        ----------
        ds : xr.Dataset
            The dataset to update.
        freq_used : list[AggregationType]
            List of aggregation frequencies used.
        latest_timestamps : dict[AggregationType, pd.Timestamp]
            Dictionary of latest timestamps for each frequency.
        """
        ds[self.var_name].attrs.update(
            {
                "long_name": self.xindice.long_name,
                "units": self.xindice.units,
                "vars2use": ", ".join([v.name for v in self.xindice.vars2use]),
            }
        )
        ds.time.attrs["long_name"] = (
            "Reference date (first day of period). "
            "DecFeb (DJF) starts in December but year reference is January of next year"
        )
        ds.time_filter.attrs.update(
            {
                "long_name": "Period identifier",
                "valid_values": ", ".join([m for m in TIME_FILTER]),
            }
        )
        # Store the latest timestamps calculated for each AggregationType
        latest_times_attrs = {
            f"last_checkpoint_{freq.value}": latest_timestamps[freq].isoformat()
            for freq in freq_used
        }
        ds.attrs.update(
            {
                "frequencies_used": ", ".join([f.value for f in freq_used]),
                "source": "ingestion-pipeline.DatasetCombiner",
                "description": "Dataset combining different frequency aggregations",
                **latest_times_attrs,
            }
        )
