common_fields_era5 = ["year", "area", "variable", "day", "product_type", "month"]

non_common_fields_per_era5_dataset = {
    "derived-era5-single-levels-daily-statistics": [
        "frequency",
        "daily_statistic",
        "time_zone",
    ],
    "derived-era5-pressure-levels-daily-statistics": [
        "daily_statistic",
        "frequency",
        "pressure_level",
        "time_zone",
    ],
    "reanalysis-era5-single-levels": ["data_format", "download_format", "time"],
    "reanalysis-era5-pressure-levels": [
        "data_format",
        "pressure_level",
        "download_format",
        "time",
    ],
}


variable_to_request = {
    "derived-era5-single-levels-daily-statistics": {
        "tas": "2m_temperature",
        "tasmax": "maximum_2m_temperature_since_previous_post_processing",
        "tasmin": "minimum_2m_temperature_since_previous_post_processing",
        "pr": "total_precipitation",
        "sfcWind": [
            "10m_u_component_of_neutral_wind",
            "10m_v_component_of_neutral_wind",
        ],
    }
}

daily_statistic_to_request = {
    "derived-era5-single-levels-daily-statistics": {
        "tas": "daily_mean",
        "tasmax": "daily_maximum",
        "tasmin": "daily_minimum",
        "pr": "daily_sum",
        "sfcWind": "daily_mean",
    }
}
