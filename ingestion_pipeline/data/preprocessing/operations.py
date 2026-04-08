import numpy as np


def sfcwind_from_u_v(ds):
    """
    Calculate wind speed from components u (u10n) and v (v10n).

    Parameters
    ----------
    ds : xarray.Dataset
        Dataset containing 'u10n' and 'v10n' wind components.

    Returns
    -------
    xarray.Dataset
        Dataset with a new 'sfcWind' variable and removed 'u10n'/'v10n' components.
    """
    sfcwind = np.power(np.power(ds["u10n"], 2) + np.power(ds["v10n"], 2), 0.5)
    ds["sfcWind"] = sfcwind
    ds["sfcWind"].attrs = ds["u10n"].attrs.copy()
    ds["sfcWind"].attrs["long_name"] = "10 metre wind speed"
    ds["sfcWind"].attrs["units"] = "m s-1"
    ds["sfcWind"].attrs["standard_name"] = "sfcWind"
    ds = ds.drop_vars(["u10n", "v10n"])
    return ds


ApplyTransformation = {"sfcWind": sfcwind_from_u_v}
