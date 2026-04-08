import calendar
import os
from datetime import datetime
from typing import Optional


def generate_filename(
    saving_directory: str,
    saving_temporal_aggregation: str,
    dataset: str,
    date: datetime,
    variable: str,
    pressure_level: Optional[str] = None,
    file_format: str = "nc",
) -> str:
    """
    Generate a filename for a given date and variable.

    Parameters
    ----------
    saving_directory : str
        The base directory where the file will be saved.
    saving_temporal_aggregation : str
        Temporal aggregation level ("daily", "monthly", or "yearly").
    dataset : str
        The name of the dataset.
    date : datetime
        The date for the file.
    variable : str
        The variable being downloaded.
    pressure_level : str, optional
        The pressure level being downloaded (default is None).

    Returns
    -------
    str
        The generated file path.
    """
    pressure_level_str = f"{pressure_level}" if pressure_level else "None"

    if date is None:
        date_string = "None"
        folder_path = os.path.join(
            saving_directory, dataset, variable, pressure_level_str
        )
    else:
        folder_path = os.path.join(
            saving_directory, dataset, variable, pressure_level_str, date.strftime("%Y")
        )
        os.makedirs(folder_path, exist_ok=True)
        date_string = get_datetime_string_for_filename(
            saving_temporal_aggregation, date
        )

    filename = f"{variable}_{pressure_level_str}_{dataset}_{date_string}.{file_format}"
    return os.path.join(folder_path, filename)


def parse_filename(
    file_path: str,
) -> tuple[str, str, str, datetime, str, Optional[str]]:
    """
    Parse a given file path to extract components.

    Parameters
    ----------
    file_path : str
        The full file path to parse.

    Returns
    -------
    tuple
        A tuple containing:
        - saving_directory (str): The base directory.
        - saving_temporal_aggregation (str):
          Temporal aggregation level ("daily", "monthly", "yearly").
        - dataset (str): The name of the dataset.
        - date (datetime): The date for the file.
        - variable (str): The variable in the file.
        - pressure_level (str or None): The pressure level (if any).

    """
    # Extract the directory and filename
    folder_path, filename = os.path.split(file_path)

    # Extract the saving_directory by removing dataset, variable, and year
    folder_parts = folder_path.split(os.sep)
    if len(folder_parts) < 3:
        raise ValueError("File path structure is not recognized.")

    saving_directory = os.sep.join(folder_parts[:-4])
    dataset = folder_parts[-4]
    variable = folder_parts[-3]

    # Extract the components from the filename
    name_parts = filename.split("_")
    if len(name_parts) < 3:
        raise ValueError("Filename structure is not recognized.")

    variable_from_file = name_parts[0].rstrip("0123456789")
    if variable != variable_from_file:
        raise ValueError("Variable in folder path and filename do not match.")

    pressure_level = None
    if variable_from_file != name_parts[0]:
        pressure_level = name_parts[0][len(variable) :]

    dataset_from_file = name_parts[2]
    if dataset != dataset_from_file:
        raise ValueError("Dataset in folder path and filename do not match.")

    date_string = name_parts[3].split(".")[0]

    # Determine temporal aggregation and parse date accordingly
    saving_temporal_aggregation, date = _parse_date_and_aggregation(date_string)

    return (
        saving_directory,
        saving_temporal_aggregation,
        dataset,
        date,
        variable,
        pressure_level,
    )


def _parse_date_and_aggregation(date_string: str) -> tuple[str, datetime]:
    """Parse the date string to determine temporal aggregation and date."""
    if len(date_string) == 8 and date_string.isdigit():
        return "daily", datetime.strptime(date_string, "%Y%m%d")

    if "-" in date_string:
        start_str, end_str = date_string.split("-")
        if len(start_str) == 8 and len(end_str) == 8:
            start_date = datetime.strptime(start_str, "%d%m%Y")
            end_date = datetime.strptime(end_str, "%d%m%Y")

            # Infer aggregation
            if (
                start_date.day == 1
                and end_date.day == 31
                and start_date.month == 1
                and end_date.month == 12
            ):
                return "yearly", start_date
            elif start_date.day == 1 and end_date.month == start_date.month:
                return "monthly", start_date
            else:
                raise ValueError("Cannot infer temporal aggregation from date range.")
        else:
            raise ValueError("Date range format not recognized.")
    else:
        raise ValueError("Date format in filename is not recognized.")


def get_datetime_string_for_filename(
    saving_temporal_aggregation: str, date: datetime
) -> str:
    """
    Generate a date string for use in a filename based on the temporal aggregation.

    Parameters
    ----------
    saving_temporal_aggregation : str
        Temporal aggregation level ("daily", "monthly", or "yearly").
    date : datetime
        The date to format into a string range.

    Returns
    -------
    str
        A string representing the date range:
        - "daily": YYYYMMDD
        - "monthly": DDMMYYYY-DDMMYYYY (e.g., 01011950-31011950)
        - "yearly": DDMMYYYY-DDMMYYYY (e.g., 01011950-31121950)

    Raises
    ------
    ValueError
        If an unsupported temporal aggregation is provided.
    """
    if saving_temporal_aggregation == "daily":
        return date.strftime("%Y%m%d")

    elif saving_temporal_aggregation == "monthly":
        first_day = date.replace(day=1)
        last_day = date.replace(day=calendar.monthrange(date.year, date.month)[1])
        return f"{first_day.strftime('%d%m%Y')}-{last_day.strftime('%d%m%Y')}"

    elif saving_temporal_aggregation == "yearly":
        first_day = date.replace(month=1, day=1)
        last_day = date.replace(month=12, day=31)
        return f"{first_day.strftime('%d%m%Y')}-{last_day.strftime('%d%m%Y')}"

    else:
        raise ValueError(
            "Unsupported saving temporal aggregation. "
            "Choose from: 'daily', 'monthly', 'yearly'. "
            f"You selected: {saving_temporal_aggregation}"
        )
