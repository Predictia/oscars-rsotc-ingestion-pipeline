import logging

import xarray

logger = logging.getLogger(__name__)


CELSIUS = "Celsius"
GRAMS_PER_KG = "gr kg-1"
KG_PER_SQUARE_METER = "kg m-2"
KG_PER_SQUARE_METER_PER_SECOND = "kg m-2 s-1"
METERS_PER_SECOND = "m s-1"
METERS_PER_SECOND_SCIENTIFIC = "m s**-1"
MILLIMETERS = "mm"
PA = "Pa"
PERCENT = "%"
WATTS_PER_SQUARE_METER = "W m-2"

UNIT_CONVERTER = {
    "Kelvin": (1, -273.15, CELSIUS),
    "kelvin": (1, -273.15, CELSIUS),
    "1": (1000, 0, GRAMS_PER_KG),
    "K": (1, -273.15, CELSIUS),
    "Fahrenheit": (5 / 9, -32 * 5 / 9, CELSIUS),
    CELSIUS: (1, 0, CELSIUS),
    "kg kg-1": (1000, 0, GRAMS_PER_KG),
    "degrees Celsius": (1, 0, CELSIUS),
    "degC": (1, 0, CELSIUS),
    "m hour**-1": (1000 * 24, 0, MILLIMETERS),
    "mm day**-1": (1, 0, MILLIMETERS),
    "mm/day": (1, 0, MILLIMETERS),
    MILLIMETERS: (1, 0, MILLIMETERS),
    "m": (1000, 0, MILLIMETERS),
    "mm s**-1": (3600 * 24, 0, MILLIMETERS),
    "m**3 m**-3": (100, 0, KG_PER_SQUARE_METER),
    "kg m**-2 day**-1": (1, 0, MILLIMETERS),
    KG_PER_SQUARE_METER_PER_SECOND: (3600 * 24, 0, MILLIMETERS),
    "kg m**-2": (1, 0, MILLIMETERS),
    KG_PER_SQUARE_METER: (1, 0, MILLIMETERS),
    "C": (1, 0, CELSIUS),
    "(0 - 1)": (100, 0, PERCENT),
    "Fraction": (100, 0, PERCENT),
    "m of water equivalent": (1000, 0, MILLIMETERS),
    METERS_PER_SECOND_SCIENTIFIC: (1, 0, METERS_PER_SECOND),
    METERS_PER_SECOND: (1, 0, METERS_PER_SECOND),
    "m/s": (1, 0, METERS_PER_SECOND),
    "km h**-1": (10 / 36, 0, METERS_PER_SECOND),
    "W/m2": (1, 0, WATTS_PER_SQUARE_METER),
    PA: (0.01, 0, "hPa"),
    "knots": (0.51, 0, METERS_PER_SECOND),
    "kts": (0.51, 0, METERS_PER_SECOND),
    "mph (nautical miles per hour)": (0.51, 0, METERS_PER_SECOND),
    PERCENT: (1, 0, PERCENT),
    "W m**-2": (1, 0, WATTS_PER_SQUARE_METER),
    "J m**-2": (1 / 3600, 0, WATTS_PER_SQUARE_METER),
}

UNIT_CONVERTER_HOURLY_TO_DAILY = {
    "mm s**-1": (3600, 0, MILLIMETERS),
    "m hour**-1": (1000, 0, MILLIMETERS),
    KG_PER_SQUARE_METER_PER_SECOND: (3600, 0, MILLIMETERS),
}
UNIT_CONVERTER_SPECIAL = {
    KG_PER_SQUARE_METER_PER_SECOND: (-3600, 0, MILLIMETERS),
}
UNIT_CONVERTER_MONTHLY = {
    "J m**-2": (1 / (3600 * 24), 0, WATTS_PER_SQUARE_METER),
    "m": (-1000, 0, MILLIMETERS),
    "m of water equivalent": (-1000, 0, MILLIMETERS),
}
VALID_UNITS = {
    "tas": CELSIUS,
    "t2m": CELSIUS,
    "2t": CELSIUS,
    "mx2t": CELSIUS,
    "tasmax": CELSIUS,
    "tasmin": CELSIUS,
    "hurs": PERCENT,
    "clt": PERCENT,
    "evspsbl": MILLIMETERS,
    "pr": MILLIMETERS,
    "psl": "hPa",
    "prsn": MILLIMETERS,
    "siconc": PERCENT,
    "sfcWind": METERS_PER_SECOND,
    "uwind": METERS_PER_SECOND_SCIENTIFIC,
    "vwind": METERS_PER_SECOND_SCIENTIFIC,
    "mrsos": KG_PER_SQUARE_METER,
    "mrro": KG_PER_SQUARE_METER,
    "huss": GRAMS_PER_KG,
    "sst": CELSIUS,
    "rlds": WATTS_PER_SQUARE_METER,
    "rsds": WATTS_PER_SQUARE_METER,
    "ps": PA,
    "2d": CELSIUS,
    "mslp": PA,
    "z": "m**2 s**-2",
}


def convert_units(ds: xarray.Dataset) -> xarray.Dataset:
    """
    Transform the data units of the dataset variables.

    Performs data transformation by reading the 'units' attribute inside the metadata
    and applying conversion factors defined in UNIT_CONVERTER. For instance, if data
    is in Kelvin, it converts it to Celsius.

    Parameters
    ----------
    ds : xarray.Dataset
        The input dataset containing variables with units to be converted.

    Returns
    -------
    xarray.Dataset
        The dataset with variables converted to the target units and updated metadata.
    """
    for ds_var in list(ds.data_vars):
        if ds_var not in VALID_UNITS:
            logger.warning(
                f"Variable {ds_var} not found in VALID_UNITS. Skipping unit conversion."
            )
            continue
        var_attrs = ds[ds_var].attrs.copy()
        if "units" not in var_attrs:
            logger.warning(
                f"Variable {ds_var} has no units attribute. Skipping unit conversion."
            )
            continue
        if var_attrs["units"] == VALID_UNITS[ds_var]:
            logger.info(
                f"The dataset {ds_var} units are already in the correct magnitude"
            )
        else:
            logger.info(
                f"The dataset {ds_var} units are not in the correct magnitude. "
                f'A conversion from {var_attrs["units"]} to '
                f"{VALID_UNITS[ds_var]} will be performed."
            )
            conversion = UNIT_CONVERTER[ds[ds_var].attrs["units"]]
            ds[ds_var] = ds[ds_var] * conversion[0] + conversion[1]
            var_attrs["units"] = conversion[2]
        ds[ds_var].attrs = var_attrs
    return ds
