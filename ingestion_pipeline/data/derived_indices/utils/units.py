import logging

from xarray import Dataset
from xclim.core.units import check_units as xclim_check_units

from ingestion_pipeline.data.derived_indices.indices.data_models import Index

logger = logging.getLogger(__name__)


def validate_and_fix_units(ds: Dataset, index: Index) -> Dataset:
    """
    Check units and update dataset attributes if needed.

    This method doesn't trigger computation on Dask arrays - it only modifies metadata.

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset to check.
    index : Index
        Index object containing variable specifications.

    Returns
    -------
    xarray.Dataset
        Dataset with updated unit attributes (lazy, metadata-only).
    """
    # Identify which variables need unit updates
    attrs_to_update = {}

    for variable in index.vars2use:
        name = variable.name

        if name not in ds:
            raise KeyError(f"Variable '{name}' not found in dataset.")

        units_in_input = ds[name].attrs.get("units")
        if units_in_input is None:
            raise RuntimeError(f"Variable '{name}' is missing a 'units' attribute.")

        # Validate units using xclim
        try:
            xclim_check_units(val=units_in_input, dim=variable.units)
        except ValueError:
            # Units mismatch → schedule fix
            attrs_to_update[name] = variable.units
            logger.info(
                f"Updating units for {name}: {units_in_input} -> {variable.units}"
            )

    # Apply updates lazily (no data copy)
    if attrs_to_update:
        ds = ds.copy(deep=False)
        for var, new_units in attrs_to_update.items():
            ds[var].attrs["units"] = new_units

    return ds
