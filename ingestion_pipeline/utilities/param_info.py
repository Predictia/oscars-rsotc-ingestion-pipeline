from typing import Optional

import pandas
import requests
import xarray


class ParamInfo:
    """
    A class to represent parameter information.

    Attributes
    ----------
    param_id : int
        The parameter ID.
    name : str
        The full name of the parameter.
    short_name : str
        The abbreviated name of the parameter.
    units : str
        The units associated with the parameter.
    description : str
        A detailed description of the parameter.
    """

    def __init__(self, param_id, long_name, short_name, units, description):
        self.param_id = param_id
        self.long_name = long_name
        self.short_name = short_name
        self.units = units
        self.description = description


def get_param_information(ds: xarray.Dataset) -> Optional[dict[str, ParamInfo]]:
    """
    Retrieve parameter information from a xarray Dataset's GRIB metadata.

    Parameters
    ----------
    ds : xarray.Dataset
        The dataset containing GRIB-encoded variables.

    Returns
    -------
    Optional[dict[str, ParamInfo]]
        An instance of ParamInfo with detailed parameter information,
        or None if the parameter ID is not found in the dataset's metadata.
    """
    # Extract the first variable name from the dataset
    var_names = list(ds.data_vars)

    parameters_info = {}
    for var_name in var_names:
        # Access the attributes of the variable
        attrs = ds[var_name].attrs

        # Check if the GRIB_paramId attribute is present
        if "GRIB_paramId" in attrs:
            param_id = attrs["GRIB_paramId"]  # Extract the parameter ID
        else:
            return None  # Return None if GRIB_paramId is missing

        # Fetch parameter and unit information from ECMWF's API
        r_table = requests.get(
            "https://codes.ecmwf.int/parameter-database/api/v1/param/?regex=false&all=true"
        )
        r_units = requests.get(
            "https://codes.ecmwf.int/parameter-database/api/v1/unit/?ordering=name"
        )

        # Parse JSON responses into tables
        table = r_table.json()["results"]
        table_units = r_units.json()

        # Convert tables to pandas DataFrames
        df_info = pandas.DataFrame(table)
        df_units = pandas.DataFrame(table_units)

        # Filter the parameter information by the parameter ID
        parameter_info = df_info[df_info["id"] == param_id]

        # Retrieve the associated unit name
        parameter_units = df_units[
            df_units["id"] == int(parameter_info["unit_id"].values[0])
        ]["name"].values[0]

        # Create an instance of ParamInfo with detailed information
        parameter = ParamInfo(
            param_id=param_id,
            long_name=parameter_info["name"].values[0],
            short_name=parameter_info["shortname"].values[0],
            units=parameter_units,
            description=parameter_info["description"].values[0],
        )
        parameters_info[var_name] = parameter

    return parameters_info
