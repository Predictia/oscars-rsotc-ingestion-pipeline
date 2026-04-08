import logging

import xarray

logger = logging.getLogger(__name__)


def rename_time_dimension(ds: xarray.Dataset) -> xarray.Dataset:
    """
    Rename time-related dimensions for consistency.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to process.

    Returns
    -------
    xarray.Dataset
        Dataset with renamed time dimension.
    """
    logger.info("Renaming time dimension.")
    if "valid_time" in ds.dims:
        ds = ds.rename({"valid_time": "time"})
    return ds


def rename_spatial_dimensions(dataset: xarray.Dataset) -> xarray.Dataset:
    """
    Fix the names for spatial coordinates (x, y, lon, lat, ...).

    Attempts multiple rename strategies to standardize spatial coordinates
    into 'lon', 'lat', 'x', and 'y'.

    Parameters
    ----------
    dataset : xarray.Dataset
        The input dataset with potentially non-standard coordinate names.

    Returns
    -------
    xarray.Dataset
        The dataset with standardized spatial coordinates.

    Raises
    ------
    NotImplementedError
        If none of the available strategies match the dataset's coordinate structure.
    """
    strategies = [
        _strategy_rlon_longitude,
        _strategy_nav_lat,
        _strategy_i_longitude,
        _strategy_i_lon,
        _strategy_rlon_only,
        _strategy_nj_ni,
        _strategy_lon_correct,
        _strategy_longitude_only,
        _strategy_x_y,
    ]

    for strategy in strategies:
        result = strategy(dataset)
        if result is not None:
            return result

    raise NotImplementedError


def _strategy_rlon_longitude(dataset):
    """
    Rename coordinates from rlon/rlat and longitude/latitude to x/y and lon/lat.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if ("rlon" in dataset.dims or "rlon" in dataset.coords) and (
        "longitude" in dataset.dims or "longitude" in dataset.coords
    ):
        logger.info(
            'Fixing coordinates names from "rlon" to "x", "rlat" to "y", '
            '"longitude" to "lon", and latitude to "lat"'
        )
        return dataset.rename(
            {"rlon": "x", "rlat": "y", "longitude": "lon", "latitude": "lat"}
        )
    return None


def _strategy_nav_lat(dataset):
    """
    Rename coordinates from nav_lon/nav_lat to lon/lat.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if "nav_lat" in dataset.coords:
        logger.info(
            'Fixing coordinates names from "nav_lat" to "lat" and "nav_lon" to "lon"'
        )
        return dataset.rename({"nav_lon": "lon", "nav_lat": "lat"})
    return None


def _strategy_i_longitude(dataset):
    """
    Rename coordinates from i/j and longitude/latitude to x/y and lon/lat.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if ("i" in dataset.dims or "i" in dataset.coords) and (
        "longitude" in dataset.dims or "longitude" in dataset.coords
    ):
        logger.info(
            'Fixing coordinates names from "i" to "x", "j" to "y", '
            '"longitude" to "lon", and latitude to "lat"'
        )
        return dataset.rename(
            {"i": "x", "j": "y", "longitude": "lon", "latitude": "lat"}
        )
    return None


def _strategy_i_lon(dataset):
    """
    Rename coordinates from i/j and lon/lat to x/y and lon/lat.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if ("i" in dataset.dims or "i" in dataset.coords) and (
        "lon" in dataset.dims or "lon" in dataset.coords
    ):
        logger.info(
            'Fixing coordinates names from "i" to "x", "j" to "y", '
            '"lon" to "lon", and lat to "lat"'
        )
        return dataset.rename({"i": "x", "j": "y", "lon": "lon", "lat": "lat"})
    return None


def _strategy_rlon_only(dataset):
    """
    Rename coordinates from rlon/rlat to x/y.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if "rlon" in dataset.dims or "rlon" in dataset.coords:
        logger.info('Fixing coordinates names from "rlon" to "x", and "rlat" to "y"')
        return dataset.rename({"rlon": "x", "rlat": "y"})
    return None


def _strategy_nj_ni(dataset):
    """
    Rename coordinates from ni/nj to x/y.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if "nj" in dataset.dims or "nj" in dataset.coords:
        logger.info('Fixing coordinates names from "ni" to "x", and "nj" to "y"')
        return dataset.rename({"ni": "x", "nj": "y"})
    return None


def _strategy_lon_correct(dataset):
    """
    Verify if dataset already has lon/lat coordinate names.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Input dataset if it has lon/lat, else None.
    """
    if "lon" in dataset.dims or "lon" in dataset.coords:
        logger.info(
            "Dataset has already the correct names for its coordinates: "
            '"lon" and "lat"'
        )
        return dataset
    return None


def _strategy_longitude_only(dataset):
    """
    Rename coordinates from longitude/latitude to lon/lat.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if "longitude" in dataset.dims or "longitude" in dataset.coords:
        logger.info(
            "Fixing coordinates names from "
            '"longitude" to "lon", and "latitude" to "lat"'
        )
        return dataset.rename({"longitude": "lon", "latitude": "lat"})
    return None


def _strategy_x_y(dataset):
    """
    Rename coordinates from x/y to lon/lat.

    Parameters
    ----------
    dataset : xarray.Dataset
        Input dataset.

    Returns
    -------
    xarray.Dataset or None
        Renamed dataset if strategy matches, else None.
    """
    if "x" in dataset.dims or "y" in dataset.coords:
        logger.info('Fixing coordinates names from "x" to "lon", and "y" to "lat"')
        return dataset.rename({"x": "lon", "y": "lat"})
    return None
