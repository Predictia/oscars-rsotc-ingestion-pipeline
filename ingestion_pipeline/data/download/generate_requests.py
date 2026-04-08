import pandas

from ingestion_pipeline.data.download.request_static_info import (
    daily_statistic_to_request,
    non_common_fields_per_era5_dataset,
    variable_to_request,
)
from ingestion_pipeline.utilities.filename import generate_filename


def generate_requests(
    dataset,
    variable,
    pressure_level,
    area,
    saving_temporal_aggregation,
    saving_main_directory,
    start_date,
    end_date,
):
    """
    Generate download requests for ERA5 data.

    Parameters
    ----------
    dataset : str
        Name of the dataset.
    variable : list of str
        Variables to download.
    pressure_level : list of str
        Pressure levels to include.
    area : list of float
        Spatial subset [North, West, South, East].
    saving_temporal_aggregation : str
        Temporal aggregation (daily, monthly, or yearly).
    saving_main_directory : str
        Directory to save files.
    start_date : str
        Start date (YYYY-MM-DD).
    end_date : str
        End date (YYYY-MM-DD).

    Returns
    -------
    list of dict
        List of requests for downloading.
    """
    dates_to_download = pandas.date_range(
        start=start_date,
        end=end_date,
        freq={"daily": "D", "monthly": "MS", "yearly": "YS"}[
            saving_temporal_aggregation
        ],
    )
    requests = [
        _create_request_for_date(
            date,
            dataset,
            variable,
            pressure_level,
            area,
            saving_temporal_aggregation,
            saving_main_directory,
        )
        for date in dates_to_download
    ]
    return requests


def _create_request_for_date(
    date,
    dataset,
    variable,
    pressure_level,
    area,
    saving_temporal_aggregation,
    saving_main_directory,
):
    """
    Create a request for a specific date.

    Parameters
    ----------
    date : pandas.Timestamp
        Date to process.
    dataset : str
        Name of the dataset.
    variable : list of str
        Variables to download.
    pressure_level : list of str
        Pressure levels to include.
    area : list of float
        Spatial subset [North, West, South, East].
    saving_temporal_aggregation : str
        Temporal aggregation (daily, monthly, yearly).
    saving_main_directory : str
        Directory to save files.

    Returns
    -------
    dict
        Request dictionary.
    """
    day, month = get_day_and_month_values(date, saving_temporal_aggregation)
    request = {
        "product_type": "reanalysis",
        "variable": variable_to_request[dataset][variable],
        "year": [str(date.year)],
        "month": month,
        "day": day,
        "area": area,
    }
    non_common_fields = non_common_fields_per_era5_dataset[dataset]
    if "pressure_level" in non_common_fields:
        request["pressure_level"] = [pressure_level]

    if "time" in non_common_fields:
        request["time"] = [f"{hour:02d}:00" for hour in range(24)]

    if "daily_statistic" in non_common_fields:
        request["daily_statistic"] = daily_statistic_to_request[dataset][variable]

    if "time_zone" in non_common_fields:
        request["time_zone"] = "utc+00:00"
    if "frequency" in non_common_fields:
        request["frequency"] = "1_hourly"

    # Adding data and download format
    if "data_format" in non_common_fields:
        request["data_format"] = "netcdf"
    if "download_format" in non_common_fields:
        request["download_format"] = "unarchived"

    if isinstance(variable_to_request[dataset][variable], list):
        file_format = "zip"
    else:
        file_format = "nc"

    filename = generate_filename(
        saving_main_directory,
        saving_temporal_aggregation,
        "ERA5",
        date,
        variable,
        pressure_level,
        file_format=file_format,
    )
    return {"file": filename, "catalogue_entry": dataset, "request": request}


def get_day_and_month_values(date, saving_temporal_aggregation):
    """
    Get day and month values based on temporal aggregation.

    Parameters
    ----------
    date : pandas.Timestamp
        Date to process.
    saving_temporal_aggregation : str
        Temporal aggregation (daily, monthly, yearly).

    Returns
    -------
    tuple of list
        Day and month values for the request.
    """
    if saving_temporal_aggregation == "daily":
        return [f"{date.day:02d}"], [f"{date.month:02d}"]
    elif saving_temporal_aggregation == "monthly":
        return [
            f"{d.day:02d}"
            for d in pandas.date_range(
                start=date.replace(day=1),
                end=(date + pandas.offsets.MonthEnd(1)),
                freq="D",
            )
        ], [f"{date.month:02d}"]
    elif saving_temporal_aggregation == "yearly":
        return [f"{d:02d}" for d in range(1, 32)], [f"{m:02d}" for m in range(1, 13)]
    else:
        raise ValueError("Saving temporal aggregation not supported.")
