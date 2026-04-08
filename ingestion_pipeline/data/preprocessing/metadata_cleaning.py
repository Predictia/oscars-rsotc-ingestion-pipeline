import logging
from typing import Optional

import xarray

logger = logging.getLogger(__name__)


def remove_all_global_attrs(ds: xarray.Dataset) -> xarray.Dataset:
    """
    Remove all global attributes from the dataset.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to clean.

    Returns
    -------
    xarray.Dataset
        Cleaned dataset.
    """
    logger.info("Removing all global attributes.")
    ds.attrs = {}
    return ds


def remove_all_var_and_coord_grib_attrs(ds: xarray.Dataset) -> xarray.Dataset:
    """
    Remove all variable and coordinate attributes from the dataset.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to clean.

    Returns
    -------
    xarray.Dataset
        Cleaned dataset.
    """
    logger.info("Removing all variable and coordinate attributes.")
    for var in ds.data_vars:
        var_attrs = ds[var].attrs.copy()
        for var_attr in ds[var].attrs:
            if var_attr.startswith("GRIB_"):
                del var_attrs[var_attr]
        ds[var].attrs = var_attrs
    for coord in ds.coords:
        coord_attrs = ds[coord].attrs.copy()
        for coord_attr in ds[coord].attrs:
            if coord_attr.startswith("GRIB_"):
                del coord_attrs[coord_attr]
        ds[coord].attrs = coord_attrs
    return ds


def get_dublin_core_metadata(
    variable_name: str,
    start_date: str,
    end_date: str,
    region_set: Optional[str] = None,
    area: Optional[list[float]] = None,
    long_name: Optional[str] = None,
    description: Optional[str] = None,
    frequency: str = "daily",
) -> dict[str, str]:
    """
    Generate Dublin Core metadata for the dataset.

    Parameters
    ----------
    variable_name : str
        Name of the main variable.
    start_date : str
        Start date (YYYY-MM-DD).
    end_date : str
        End date (YYYY-MM-DD).
    region_set : str, optional
        Region set name if applicable.
    area : list of float, optional
        Spatial subset [North, West, South, East].
    long_name : str, optional
        Custom long name for the dataset title.
    description : str, optional
        Custom description for the dataset.
    frequency : str, optional
        Temporal frequency of the data (e.g., 'daily', 'monthly'). Default is 'daily'.

    Returns
    -------
    dict[str, str]
        Dictionary of Dublin Core metadata.
    """
    from datetime import datetime

    long_name = long_name or variable_name
    now = datetime.now().isoformat() + "Z"
    start_year = start_date[:4]
    end_year = end_date[:4]

    # Handle multiple frequencies
    freq_label = frequency
    if "," in frequency:
        freq_label = "multi-frequency"
        freq_desc = f"multi-frequency ({frequency}) aggregated"
    else:
        freq_desc = f"{frequency.capitalize()} aggregated"

    if region_set:
        alternative = f"ERA5 aggregated by NUTS levels - {long_name} ({variable_name}) - {freq_label}"
        default_description = (
            f"{freq_desc} ERA5 {long_name} ({variable_name}) data for European {region_set} "
            "regions."
        )
    else:
        alternative = f"ERA5 gridded - {long_name} ({variable_name}) - {freq_label}"
        default_description = (
            f"{freq_desc} ERA5 {long_name} ({variable_name}) gridded data for Europe."
        )

    description = description or default_description

    if area:
        n, w, s, e = area
        spatial = f"westlimit={w}, eastlimit={e}, southlimit={s}, northlimit={n}"
    else:
        spatial = "Europe"

    dc_metadata = {
        "dc:title": f"RSOTC: {long_name}",
        "dcterms:alternative": alternative,
        "dc:description": description,
        "dc:creator": "Predictia Intelligent Data Solutions S.L. (predictia@predictia.es; https://www.predictia.es)",
        "dc:contributor": "Instituto de Física de Cantabria (IFCA; https://ifca.unican.es)",
        "dc:publisher": "Regional State of the Climate (RSOTC) Project",
        "dc:subject": f"climate change, ERA5, {variable_name}, NUTS, Europe",
        "dcterms:created": now,
        "dcterms:valid": f"{start_date}/{end_date}",
        "dcterms:modified": now,
        "dc:type": "Dataset",
        "dc:format": "application/zarr",
        "dcterms:medium": "Cloud Storage (S3)",
        "dc:source": "Copernicus Climate Data Store (https://doi.org/10.24381/cds.4991cf48)",
        "dc:language": "en",
        "dcterms:conformsTo": "CF-1.8; RO-Crate 1.1",
        "dc:coverage": f"{spatial}; {start_year}-{end_year}",
        "dcterms:temporal": f"start={start_date}; end={end_date}",
        "dcterms:spatial": spatial,
        "dc:rights": "Apache License, Version 2.0",
        "dcterms:accessRights": "Open Access",
        "dcterms:license": "https://www.apache.org/licenses/LICENSE-2.0",
        "dcterms:accrualMethod": "Automated monthly ingestion via CDS API",
        "dcterms:accrualPeriodicity": "Monthly",
    }

    return dc_metadata


def apply_dublin_core_metadata(
    ds: xarray.Dataset,
    variable_name: str,
    start_date: str,
    end_date: str,
    region_set: Optional[str] = None,
    area: Optional[list[float]] = None,
    long_name: Optional[str] = None,
    description: Optional[str] = None,
    frequency: str = "daily",
) -> xarray.Dataset:
    """
    Clear all global metadata and apply Dublin Core standard.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to process.
    variable_name : str
        Main variable name.
    start_date : str
        Start date.
    end_date : str
        End date.
    region_set : str, optional
        Region set identifier.
    area : list of float, optional
        Spatial bounding box.
    long_name : str, optional
        Custom long name for the dataset title.
    description : str, optional
        Custom description for the dataset.
    frequency : str, optional
        Temporal frequency of the data.

    Returns
    -------
    xarray.Dataset
        Dataset with new metadata.
    """
    if long_name is None and variable_name in ds.data_vars:
        long_name = ds[variable_name].attrs.get("long_name")

    # Preserve functional attributes
    functional_attrs = {
        k: v
        for k, v in ds.attrs.items()
        if k.startswith("last_checkpoint_") or k == "frequencies_used"
    }

    ds = remove_all_global_attrs(ds)
    dc_metadata = get_dublin_core_metadata(
        variable_name,
        start_date,
        end_date,
        region_set=region_set,
        area=area,
        long_name=long_name,
        description=description,
        frequency=frequency,
    )
    ds.attrs.update(dc_metadata)
    ds.attrs.update(functional_attrs)
    return ds


def update_dynamic_metadata(
    ds: xarray.Dataset,
    variable_name: str,
    start_date: str,
    end_date: str,
    region_set: Optional[str] = None,
    area: Optional[list[float]] = None,
    long_name: Optional[str] = None,
    description: Optional[str] = None,
) -> xarray.Dataset:
    """
    Update dynamic Dublin Core metadata fields.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to update.
    variable_name : str
        Main variable name.
    start_date : str
        Start date.
    end_date : str
        End date.
    region_set : str, optional
        Region set identifier.
    area : list of float, optional
        Spatial bounding box.
    long_name : str, optional
        Custom long name (propagate if needed).
    description : str, optional
        Custom description (propagate if needed).

    Returns
    -------
    xarray.Dataset
        Updated dataset.
    """
    from datetime import datetime

    now = datetime.now().isoformat() + "Z"
    start_year = start_date[:4]
    end_year = end_date[:4]

    updates = {
        "dcterms:modified": now,
        "dcterms:valid": f"{start_date}/{end_date}",
        "dcterms:temporal": f"start={start_date}; end={end_date}",
        "dc:coverage": f"Europe; {start_year}-{end_year}",
    }

    if description:
        updates["dc:description"] = description
    if long_name:
        updates["dc:title"] = f"RSOTC: {long_name}"

    ds.attrs.update(updates)
    return ds
