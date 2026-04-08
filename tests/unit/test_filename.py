from datetime import datetime

import pytest

from ingestion_pipeline.utilities.filename import (
    generate_filename,
    get_datetime_string_for_filename,
    parse_filename,
)


def test_generate_filename_daily():
    date = datetime(2023, 1, 1)
    path = generate_filename(
        saving_directory="/tmp",
        saving_temporal_aggregation="daily",
        dataset="test_ds",
        date=date,
        variable="temp",
        file_format="nc",
    )
    # Expected: /tmp/test_ds/temp/None/2023/temp_None_test_ds_20230101.nc
    assert "/tmp/test_ds/temp/None/2023" in path
    assert path.endswith("temp_None_test_ds_20230101.nc")


def test_generate_filename_monthly():
    date = datetime(2023, 1, 15)
    path = generate_filename(
        saving_directory="/tmp",
        saving_temporal_aggregation="monthly",
        dataset="test_ds",
        date=date,
        variable="temp",
    )
    # Expected: 20230101-20230131
    assert "temp_None_test_ds_01012023-31012023.nc" in path


def test_generate_filename_no_date():
    path = generate_filename(
        saving_directory="/tmp",
        saving_temporal_aggregation="daily",
        dataset="test_ds",
        date=None,
        variable="temp",
    )
    # Expected: .../temp/None/temp_None_test_ds_None.nc
    assert "/tmp/test_ds/temp/None" in path
    assert path.endswith("temp_None_test_ds_None.nc")


def test_parse_filename_valid():
    # Construct a valid path manually or use the generator
    # temp_None_dataset_20230101.nc
    # Structure: .../dataset/variable/pressure/year/filename
    path = "/tmp/data/dataset/temp/None/2023/temp_None_dataset_20230101.nc"

    saved_dir, agg, ds, date, var, pressure = parse_filename(path)

    assert ds == "dataset"
    assert var == "temp"
    assert agg == "daily"
    assert date == datetime(2023, 1, 1)
    assert pressure is None


def test_parse_filename_invalid_structure():
    with pytest.raises(ValueError, match="File path structure is not recognized"):
        parse_filename("/short/path.nc")


def test_parse_filename_invalid_filename_structure():
    # Folder structure ok, but filename too short
    path = "/tmp/data/dataset/temp/None/2023/invalid.nc"
    with pytest.raises(ValueError, match="Filename structure is not recognized"):
        parse_filename(path)


def test_parse_filename_mismatch():
    # Variable in folder != variable in file
    path = "/tmp/data/my_ds/myvar/None/2023/othervar_None_my_ds_20230101.nc"
    with pytest.raises(
        ValueError, match="Variable in folder path and filename do not match"
    ):
        parse_filename(path)

    # Dataset in folder != dataset in file
    path = "/tmp/data/my_ds/myvar/None/2023/myvar_None_other_ds_20230101.nc"
    with pytest.raises(
        ValueError, match="Dataset in folder path and filename do not match"
    ):
        parse_filename(path)


def test_parse_filename_pressure_level():
    path = "/tmp/data/dataset/temp/500/2023/temp500_500_dataset_20230101.nc"
    _, _, _, _, _, pressure = parse_filename(path)
    assert pressure == "500"


def test_parse_filename_yearly():
    path = "/tmp/data/dataset/temp/None/2023/temp_None_dataset_01012023-31122023.nc"
    _, agg, _, _, _, _ = parse_filename(path)
    assert agg == "yearly"


def test_parse_filename_monthly():
    path = "/tmp/data/dataset/temp/None/2023/temp_None_dataset_01012023-31012023.nc"
    _, agg, _, _, _, _ = parse_filename(path)
    assert agg == "monthly"


def test_parse_filename_invalid_date_range():
    # Lengths are 8, but not covering full year or full month properly
    # e.g. 02012023-05012023 (3 days) -> undetermined aggregation
    path = "/tmp/data/dataset/temp/None/2023/temp_None_dataset_02012023-05012023.nc"
    with pytest.raises(ValueError, match="Cannot infer temporal aggregation"):
        parse_filename(path)

    # Invalid range format
    path = "/tmp/data/dataset/temp/None/2023/temp_None_dataset_2023-2024.nc"
    with pytest.raises(ValueError, match="Date range format not recognized"):
        parse_filename(path)

    # Invalid general format
    path = "/tmp/data/dataset/temp/None/2023/temp_None_dataset_invalid_date.nc"
    with pytest.raises(ValueError, match="Date format in filename is not recognized"):
        parse_filename(path)


def test_get_datetime_string_for_filename_error():
    with pytest.raises(ValueError, match="Unsupported saving temporal aggregation"):
        get_datetime_string_for_filename("hourly", datetime.now())

    # Coverage for yearly
    date = datetime(2023, 5, 20)
    s = get_datetime_string_for_filename("yearly", date)
    assert s == "01012023-31122023"
