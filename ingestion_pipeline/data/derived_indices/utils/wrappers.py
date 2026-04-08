from collections.abc import Callable
from functools import partial
from typing import Any, Protocol

import xarray


class DatarrayFunction(Protocol):
    def __call__(
        self, input_da: xarray.DataArray, *args: Any, **kwds: Any
    ) -> xarray.DataArray:
        """
        Call the function on a DataArray.

        Parameters
        ----------
        input_da : xarray.DataArray
            Input data array.
        *args : Any
            Positional arguments.
        **kwds : Any
            Keyword arguments.

        Returns
        -------
        xarray.DataArray
            Resulting data array.
        """
        ...


class DatasetFunction(Protocol):
    def __call__(
        self, dataset: xarray.Dataset, *args: Any, **kwds: Any
    ) -> xarray.Dataset:
        """
        Call the function on a Dataset.

        Parameters
        ----------
        dataset : xarray.Dataset
            Input dataset.
        *args : Any
            Positional arguments.
        **kwds : Any
            Keyword arguments.

        Returns
        -------
        xarray.Dataset
            Resulting dataset.
        """
        ...


def wrapped_dataset_function(
    dataset: xarray.Dataset, funct: DatarrayFunction, *args, **kwargs
) -> xarray.Dataset:
    """
    Apply a DataArray function to a Dataset.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.
    funct : DatarrayFunction
        Function that processes a DataArray.
    *args
        Positional arguments for `funct`.
    **kwargs
        Keyword arguments for `funct`.

    Returns
    -------
    xarray.Dataset
        Dataset containing the result of the function applied to the main variable.
    """
    varname = list(dataset.data_vars)[0]
    da = dataset[varname]
    result_da = funct(da, *args, **kwargs)
    return result_da.to_dataset(name=varname)


def wrap_datarray_funct(funct: DatarrayFunction) -> DatasetFunction:
    """
    Wrap a DataArray function to operate on a Dataset.

    Parameters
    ----------
    funct : DatarrayFunction
        Function that processes a DataArray.

    Returns
    -------
    DatasetFunction
        Wrapped function that processes a Dataset.
    """
    return partial(wrapped_dataset_function, funct=funct)


#  These values assume that the input data is daily
freq2minvalues = {
    "D": {"MS": 25, "QS-DEC": 75, "YS": 350, "W-MON": 1},
    "MS": {"MS": 1, "QS-DEC": 3, "YS": 12},
}


def check_minimum_values(funct: Callable, freq: str, orig_freq: str) -> Callable:
    """
    Wrap a function to check that enough data values for the frequency are present.

    Wrap a function to be passed to resample.map in order to check that the minimum
    data values for the frequency are present.

    Parameters
    ----------
    funct : Callable
        The function to wrap.
    freq : str
        The destination frequency.
    orig_freq : str
        The original frequency of the data.

    Returns
    -------
    Callable
        The wrapped function with minimum value check.
    """

    def wrapper(input_data: xarray.Dataset, *args, **kwargs) -> xarray.Dataset:
        minvalues = freq2minvalues[orig_freq][freq]
        result = funct(input_data, *args, **kwargs)
        complete_mask = input_data.count(dim="time") >= minvalues
        result = result.where(complete_mask)
        return result

    return wrapper
