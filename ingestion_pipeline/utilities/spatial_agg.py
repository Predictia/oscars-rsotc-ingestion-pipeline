import importlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Literal, Union

import geopandas
import numpy
import regionmask
import xarray

logger = logging.getLogger(__name__)


@dataclass
class SpatialAggregation:
    """
    Aggregates xarray.Dataset data over regions.

    Attributes
    ----------
    dataset : xarray.Dataset
       The xarray dataset containing the data to be aggregated.
    polygons_info : Union[pathlib.PosixPath, str]
       The path to a GeoJSON file, Shapefile or a Well-Known Text (WKT)
       representation of the regions to be aggregated.
    """

    dataset: xarray.Dataset
    polygons_info: Union[Path, Dict[str, str]]

    def __post_init__(self):
        """Initialize the RegionAggregation object."""
        # Load the GeoJSON file into a GeoDataFrame
        self.polygons_gdf = geopandas.read_file(self.polygons_info)

    def compute(self):
        gdf = self.polygons_gdf
        dup = gdf["NAME_LATN"].duplicated(keep=False)
        gdf = gdf.copy()
        gdf["NAME_UNIQ"] = numpy.where(
            dup, gdf["NAME_LATN"] + " (" + gdf["CNTR_CODE"] + ")", gdf["NAME_LATN"]
        )
        regions = regionmask.from_geopandas(gdf, names="NAME_UNIQ", abbrevs="NUTS_ID")
        regions_mask = regions.mask_3D(self.dataset)

        # Compute latitude weights
        latitude_weights = numpy.cos(numpy.deg2rad(self.dataset["lat"]))
        nlat, nlon = len(self.dataset.lat), len(self.dataset.lon)
        latitude_weights = xarray.DataArray(
            numpy.tile(latitude_weights, nlon).reshape((nlat, nlon), order="F"),
            dims=("lat", "lon"),
            coords=dict(lon=self.dataset.lon, lat=self.dataset.lat),
        )

        # Weighted aggregation (regions that overlap)
        region_dataset = self.dataset.weighted(regions_mask * latitude_weights).mean(
            dim=("lon", "lat"), skipna=True, keep_attrs=True
        )

        region_dataset = region_dataset.swap_dims({"region": "abbrevs"})
        region_dataset = region_dataset.drop_vars(["region", "names"])
        region_dataset = region_dataset.rename_dims({"abbrevs": "region"})
        region_dataset = region_dataset.rename_vars({"abbrevs": "region"})

        # Identify missing regions (those not in the mask)
        present = set(regions_mask.region.values.tolist())
        all_regions = set(range(len(regions)))
        missing = sorted(list(all_regions - present))

        # Fill missing with nearest point
        if len(missing) > 0:
            filled = []
            for idx in missing:
                geom = gdf.iloc[idx].geometry
                bounds = geom.bounds
                nearest = self.dataset.sel(
                    lat=(bounds[1] + bounds[3]) / 2,
                    lon=(bounds[0] + bounds[2]) / 2,
                    method="nearest",
                )
                nearest = nearest.expand_dims({"region": [regions.abbrevs[idx]]})
                nearest = nearest.drop_vars(["lat", "lon"])
                filled.append(nearest)

            if filled:
                filled_ds = xarray.concat(filled, dim="region")
                region_dataset = xarray.concat(
                    [region_dataset, filled_ds], dim="region"
                )

        return region_dataset


RegionSet = Literal["NUTS-0", "NUTS-1", "NUTS-2", "NUTS-3"]


def aggregate_regions(
    dataset: xarray.Dataset, region_set: RegionSet = "NUTS-0"
) -> xarray.Dataset:
    """Aggregate a dataset spatially over predefined regions.

    Parameters
    ----------
    dataset : xarray.Dataset
        The input dataset with spatial dimensions.
    region_set : str, optional
        The name of the region set to use, must be one of
        ["NUTS-0", "NUTS-1", "NUTS-2"]

    Returns
    -------
    xarray.Dataset
        The dataset aggregated by regions.

    Raises
    ------
    KeyError
        If the specified `region_set` is not recognized.
    """
    set2filename = {
        "NUTS-0": "NUTS_RG_60M_2024_4326_LEVL_0.geojson",
        "NUTS-1": "NUTS_RG_60M_2024_4326_LEVL_1.geojson",
        "NUTS-2": "NUTS_RG_60M_2024_4326_LEVL_2.geojson",
        "NUTS-3": "NUTS_RG_60M_2024_4326_LEVL_3.geojson",
    }
    regions_file = Path(
        str(importlib.resources.files("ingestion_pipeline")),
        "resources",
        set2filename[region_set],
    )
    logger.info(f"Aggregate in regions from {region_set=}, using file: {regions_file}.")
    sa = SpatialAggregation(dataset, regions_file)
    dataset_regions = sa.compute()
    return dataset_regions
