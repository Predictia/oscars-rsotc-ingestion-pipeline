from dataclasses import dataclass
from enum import Enum
from typing import Final, TypeAlias

import numpy as np
import pandas as pd
import xarray as xr

Year: TypeAlias = int
Month: TypeAlias = int
Day: TypeAlias = int
VALID_FREQS: Final = ("MS", "QS-DEC", "YS", "W-MON", "W", "D")


@dataclass
class YearPeriod:
    start: Year | None
    end: Year | None

    def to_slice(self) -> slice:
        """
        Convert to a slice object.

        Returns
        -------
        slice
            Slice object covering the period.
        """
        if self.start is None or self.end is None:
            raise ValueError("Both start and end must be defined for YearPeriod")
        return slice(f"{self.start}-01-01T00:00", f"{self.end}-12-31T23:59")


@dataclass
class YearPeriodFlexible(YearPeriod):
    """
    A flexible year-based period selector for xarray time slicing.

    This subclass extends :class:`YearPeriod` by allowing ``start`` and ``end``
    to be optional, providing more robust behavior when selecting reference
    periods on an xarray Dataset.

    The motivation is to support workflows where:
    - No reference period is specified (use entire dataset)
    - Partial periods are specified (from start, until end, or specific year)

    The purpose of this class is to ensure that the expression::
        See: xrindices/reference.py (`get_refernece_dataset`)
        dataset_ref_period = dataset.sel(time=ref_period.to_slice())

    always works, even when the reference period is not explicitly defined.
    In contrast, :class:`YearPeriod` requires both ``start`` and ``end`` to be
    provided, which makes it impossible to select “the entire dataset” without
    rewriting the selection logic. ``YearPeriodFlexible`` removes this
    limitation.
    """

    start: Year | None = None
    end: Year | None = None

    def to_slice(self) -> slice:
        """
        Convert the period to a time slice for xarray selection.

        Returns
        -------
        slice
            A slice object compatible with xarray.Dataset.sel(time=...).
            - If both start and end are None: returns slice(None) (entire dataset)
            - If only start is specified: returns slice from start to end of dataset
            - If only end is specified: returns slice from beginning to end
            - If both specified: returns slice from start to end

        Examples
        --------
        >>> period = YearPeriodFlexible(2000, 2020)
        >>> s = period.to_slice()
        >>> # s == slice("2000-01-01T00:00", "2020-12-31T23:59")
        """
        if self.start is None and self.end is None:
            return slice(None)
        if self.start is not None and self.end is None:
            return slice(f"{self.start}-01-01T00:00", None)
        if self.start is None and self.end is not None:
            return slice(None, f"{self.end}-12-31T23:59")
        return slice(f"{self.start}-01-01T00:00", f"{self.end}-12-31T23:59")


@dataclass
class MonthDay:
    month: Month
    day: Day


@dataclass
class SeasonLimits:
    start: MonthDay
    end: MonthDay


@dataclass
class MonthRange:
    """
    Represents a range of months for temporal aggregation.

    Attributes
    ----------
    name : str
        The name of the range (e.g., "Jan", "DecFeb", "Annual").
    low : int
        The starting month number (1-12).
    high : int
        The ending month number (1-12).
    """

    name: str
    low: int
    high: int

    # Class method to find name from a month number
    @classmethod
    def name_from_month(cls, month: int, ranges: list["MonthRange"]) -> str:
        """
        Find the name of the month range containing a specific month.

        Parameters
        ----------
        month : int
            The month number (1-12).
        ranges : list[MonthRange]
            The list of month ranges to search in.

        Returns
        -------
        str
            The name of the matching month range.

        Raises
        ------
        ValueError
            If no matching month range is found.
        """
        for r in ranges:
            # handle ranges that cross year-end (like Dec-Feb)
            if r.low <= r.high:
                if r.low <= month <= r.high:
                    return r.name
            else:
                if month >= r.low or month <= r.high:
                    return r.name
        raise ValueError(f"No MonthRange found for month {month}")


class AggregationType(Enum):
    """
    Valid temporal aggregation frequencies.

    Based on: xrindices/data_models.py
    VALID_FREQS = ("MS", "QS-DEC", "YS", "W-MON", "W", "D")

    Attributes
    ----------
    MONTH_START : str
        Monthly frequency starting at month start (MS).
    QUARTER_DEC : str
        Quarterly frequency starting in December (QS-DEC).
    YEAR_START : str
        Yearly frequency starting at year start (YS).
    WEEK_MON : str
        Weekly frequency starting on Monday (W-MON).
    WEEK : str
        Weekly frequency (W).
    DAILY : str
        Daily frequency (D).
    """

    MONTHLY = "MS"
    SEASONAL = "QS-DEC"
    ANNUAL = "YS"
    WEEK_MON = "W-MON"
    WEEK = "W"
    DAILY = "D"

    def __str__(self) -> str:
        """
        Return the frequency string representation.

        Returns
        -------
        str
            The frequency string.
        """
        return self.value

    def get_expected_elements(self, sample_freq: str = "D") -> int | None:
        """
        Calculate the expected number of elements empirically.

        Creates a 10-year complete dummy DataArray with the sample frequency,
        resamples it to the aggregation frequency, counts elements in each period,
        and returns the mode (most frequently occurring count).

        Parameters
        ----------
        sample_freq : str
            The sampling frequency of the input data. The default is "D".
            Valid options are "MS", "QS-DEC", "YS", "W-MON", "W", "D".

        Returns
        -------
        int | None
            The most frequent count of elements in each aggregation period,
            or None if the calculation fails.

        """
        # Create a 10-year time index with sample frequency
        start = pd.Timestamp("2000-01-01")
        end = start + pd.DateOffset(years=10)
        time_index = pd.date_range(start=start, end=end, freq=sample_freq)

        # Create a lightweight DataArray with ones (represents samples)
        da = xr.DataArray(
            np.ones(len(time_index), dtype=np.uint8),
            coords={"time": time_index},
            dims=["time"],
        )

        # Resample to aggregation frequency and count
        counts = da.resample(time=self.value).count()

        # Get the mode (most frequently occurring count)
        # Convert to Series to use value_counts for finding the mode
        count_series = pd.Series(counts.values)
        mode_value = count_series.value_counts().idxmax()

        return int(mode_value)
