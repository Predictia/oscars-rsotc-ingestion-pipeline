import logging
import os
import sys
import warnings

import numpy as np
import xarray as xr

warnings.filterwarnings("ignore")

logger = logging.getLogger(__name__)


def compare_datasets(old_path: str, new_path: str) -> None:
    """
    Compare two xarray Zarr datasets to ensure proper updates.

    This function compares the dimensions, coordinates, data variables,
    and attributes of two datasets. It specifically checks if the new
    dataset contains the data from the old dataset and has been
    extended correctly with new time steps.

    Parameters
    ----------
    old_path : str
        The file path to the original (older) Zarr dataset.
    new_path : str
        The file path to the updated (newer) Zarr dataset.

    Returns
    -------
    None
    """
    logger.info(f"Opening old dataset: {old_path}")
    ds_old = xr.open_zarr(old_path)

    logger.info(f"Opening new dataset: {new_path}")
    ds_new = xr.open_zarr(new_path)

    print("\n" + "=" * 80)
    print(f"{'DATASET COMPARISON SUMMARY':^80}")
    print("=" * 80)

    # 1. Compare Dimensions
    print("\n[1] Dimensions:")
    dims_old = dict(ds_old.dims)
    dims_new = dict(ds_new.dims)
    print(f"    Old: {dims_old}")
    print(f"    New: {dims_new}")

    for dim, size in dims_old.items():
        if dim in dims_new:
            if size == dims_new[dim]:
                print(f"    ✅ Dimension '{dim}' size matches: {size}")
            else:
                print(
                    f"    ⚠️  Dimension '{dim}' size changed: {size} -> {dims_new[dim]}"
                )
        else:
            print(f"    ❌ Dimension '{dim}' is MISSING in new dataset")

    # 2. Compare Time Coordinate
    print("\n[2] Time Coordinate:")
    time_old = ds_old.time.values
    time_new = ds_new.time.values
    print(
        f"    Old range: {np.datetime_as_string(time_old[0], unit='D')} to "
        f"{np.datetime_as_string(time_old[-1], unit='D')} ({len(time_old)} steps)"
    )
    print(
        f"    New range: {np.datetime_as_string(time_new[0], unit='D')} to "
        f"{np.datetime_as_string(time_new[-1], unit='D')} ({len(time_new)} steps)"
    )

    # Check overlap
    overlap = np.intersect1d(time_old, time_new)
    if len(overlap) == len(time_old):
        print("    ✅ All old time steps are preserved in the new dataset.")
    else:
        print(f"    ❌ Data Loss! Only {len(overlap)}/{len(time_old)} old steps found.")

    # 3. Compare Regions
    print("\n[3] Regions:")
    if ds_old.region.equals(ds_new.region):
        print(
            f"    ✅ Region coordinates are identical ({len(ds_old.region)} regions)."
        )
    else:
        print("    ❌ Region coordinates differ!")
        added = np.setdiff1d(ds_new.region.values, ds_old.region.values)
        removed = np.setdiff1d(ds_old.region.values, ds_new.region.values)
        if len(added) > 0:
            print(f"       Added: {added}")
        if len(removed) > 0:
            print(f"       Removed: {removed}")

    # 4. Compare Data Content (for overlapping time/regions)
    print("\n[4] Data Verification (variable: 'tas'):")
    if "tas" in ds_old and "tas" in ds_new:
        # Select the slice of the new dataset that corresponds to the old time range
        # We also need to be careful if regions changed, but here they seem the same
        try:
            ds_new_subset = ds_new.sel(time=ds_old.time, region=ds_old.region)

            # check for exact equality
            xr.testing.assert_allclose(ds_old.tas, ds_new_subset.tas)
            print(
                "    ✅ Data values for the original time period are identical (no corruption)."
            )
        except AssertionError:
            print("    ❌ Data values DIFFER in the overlapping period!")
            # Calculate max difference
            diff = np.abs(ds_old.tas.values - ds_new.sel(time=ds_old.time).tas.values)
            print(f"       Max absolute difference: {np.nanmax(diff)}")
        except Exception as e:
            print(f"    ❌ Error during data comparison: {e}")
    else:
        print("    ❌ Variable 'tas' not found in one or both datasets.")

    # 5. Compare Metadata (Attributes)
    print("\n[5] Metadata (Global Attributes):")
    old_attrs = ds_old.attrs
    new_attrs = ds_new.attrs

    # Key attributes to check specifically
    check_attrs = [
        "dcterms:temporal",
        "dcterms:valid",
        "last_updated",
        "dcterms:modified",
        "dc:description",
    ]

    all_keys = sorted(list(set(old_attrs.keys()) | set(new_attrs.keys())))

    print(f"    {'Attribute':<30} | {'Status':<10} | {'New Value (truncated)'}")
    print(f"    {'-'*30}-|{'-'*10}-|{'-'*35}")

    for key in all_keys:
        val_old = old_attrs.get(key)
        val_new = new_attrs.get(key)

        status = "Match"
        if val_old is None:
            status = "ADDED"
        elif val_new is None:
            status = "REMOVED"
        elif val_old != val_new:
            status = "CHANGED"

        # We only print if changed/added/removed or if it's one of the key attributes we care about
        if status != "Match" or key in check_attrs:
            display_val = (
                str(val_new)[:35] + "..." if len(str(val_new)) > 35 else str(val_new)
            )
            print(f"    {key:<30} | {status:<10} | {display_val}")

    print("\n" + "=" * 80)


def main() -> None:
    """
    Run the Zarr comparison script.

    Set up logging and define the file paths for comparison.

    Returns
    -------
    None
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s — %(name)s — %(levelname)s — %(message)s",
    )

    # Paths provided by the user (or environment variables)
    old_path = os.environ.get("OLD_ZARR_PATH", "tas_None_ERA5_NUTS-3_old.zarr")
    new_path = os.environ.get("NEW_ZARR_PATH", "tas_None_ERA5_NUTS-3.zarr")

    try:
        compare_datasets(old_path, new_path)
    except Exception as e:
        logger.error(f"Error during comparison: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
