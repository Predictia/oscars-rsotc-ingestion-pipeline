import logging

import xarray

logger = logging.getLogger(__name__)


def reindex_latitudes(
    dataset: xarray.Dataset, latitude_name: str = "lat"
) -> xarray.Dataset:
    """
    Reorder latitude values to be in ascending order.

    Parameters
    ----------
    dataset : xarray.Dataset
        The dataset to be processed.
    latitude_name : str, optional
        The name of the latitude dimension, by default "lat".

    Returns
    -------
    xarray.Dataset
        Dataset with latitudes in ascending order.
    """
    logger.info("Reindexing latitude values to be in ascending order.")
    dataset = dataset.sortby(latitude_name)
    if "stored_direction" in dataset[latitude_name].attrs:
        dataset[latitude_name].attrs["stored_direction"] = "increasing"
    return dataset


def reindex_longitudes(
    dataset: xarray.Dataset, longitude_name: str = "lon"
) -> xarray.Dataset:
    """
    Transform longitudes from [0, 360] to [-180, 180].

    Parameters
    ----------
    dataset : xarray.Dataset
        Dataset with longitudes to adjust.
    longitude_name : str, optional
        The name of the longitude dimension, by default "lon".

    Returns
    -------
    xarray.Dataset
        Dataset with adjusted longitudes.
    """
    logger.info("Adjusting longitudes to be in the range [-180, 180].")
    lon = dataset[longitude_name]
    if lon.max().values > 180 and lon.min().values >= 0:
        dataset[longitude_name] = dataset[longitude_name].where(
            lon <= 180, other=lon - 360
        )
    dataset = dataset.reindex(
        indexers={longitude_name: sorted(dataset[longitude_name].values)}
    )
    return dataset


def reindex_realization_number(dataset: xarray.Dataset) -> xarray.Dataset:
    """
    Select a single realization member.

    Parameters
    ----------
    dataset : xarray.Dataset
        Dataset to process.

    Returns
    -------
    xarray.Dataset
        Dataset with the selected realization member.
    """
    logger.info("Selecting a single realization member.")
    if "number" in dataset.dims:
        dataset = dataset.sel(number=0).drop("number")
    elif "number" in dataset.coords:
        dataset = dataset.drop("number")
    return dataset


def reorder_dataset_dimensions(ds: xarray.Dataset) -> xarray.Dataset:
    """
    Reorder dataset dimensions for consistency.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to reorder.

    Returns
    -------
    xarray.Dataset
        Reordered dataset.
    """
    ds = ds.transpose("time", "lat", "lon")
    return ds
