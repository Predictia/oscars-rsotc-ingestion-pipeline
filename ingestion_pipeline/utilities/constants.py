import numpy

mapping_statistics = {
    "mean": numpy.mean,
    "max": numpy.max,
    "min": numpy.min,
    "std": numpy.std,
    "median": numpy.median,
    "sum": numpy.sum,
}

mapping_months = {
    1: "JAN",
    2: "FEB",
    3: "MAR",
    4: "APR",
    5: "MAY",
    6: "JUN",
    7: "JUL",
    8: "AUG",
    9: "SEP",
    10: "OCT",
    11: "NOV",
    12: "DEC",
}

time_filters = {
    "Annual": "01-12",
    "DecFeb": "12-02",
    "MarMay": "03-05",
    "JunAug": "06-08",
    "SepNov": "09-11",
    "Jan": "01-01",
    "Feb": "02-02",
    "Mar": "03-03",
    "Apr": "04-04",
    "May": "05-05",
    "Jun": "06-06",
    "Jul": "07-07",
    "Aug": "08-08",
    "Sep": "09-09",
    "Oct": "10-10",
    "Nov": "11-11",
    "Dec": "12-12",
}

periods = {
    "water_level": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
    },
    "waves_dir": {
        "baseline": "1995-2014",
    },
    "waves_dp": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "waves_dpt": {
        "baseline": "1995-2014",
    },
    "waves_fp": {
        "baseline": "1995-2014",
    },
    "waves_hs": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "waves_t02": {
        "baseline": "1995-2014",
    },
    "waves_t0m1": {
        "baseline": "1995-2014",
    },
    "waves_tp": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "waves_VPED": {
        "baseline": "1995-2014",
    },
    "waves_VTPK": {
        "baseline": "1995-2014",
    },
    "wind_100m": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "wind_10m": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "wind_150m": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "wind_200m": {
        "baseline": "1995-2014",
        "near_future": "2021-2040",
        "medium_future": "2041-2060",
        "far_future": "2081-2100",
    },
    "wind_dd": {
        "baseline": "1995-2014",
    },
}

REGION_SETS = ["NUTS-0", "NUTS-1", "NUTS-2", "NUTS-3"]

VARIABLE_ZARR_PATTERN = "{variable}_{pressure_level}_ERA5_gridded.zarr"
REGION_ZARR_PATTERN = "{variable}_{pressure_level}_ERA5_{region_set}.zarr"
