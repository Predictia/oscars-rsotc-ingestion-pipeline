import os
from datetime import datetime
from unittest.mock import patch

from ingestion_pipeline.data.download.generate_requests import generate_requests
from ingestion_pipeline.utilities.filename import generate_filename


@patch("ingestion_pipeline.ingestion.cdsapi.Client")
def test_generate_requests(mock_client, tmp_path):
    """Test the request generation logic."""
    dataset = "derived-era5-single-levels-daily-statistics"
    variable = "tasmax"
    area = ([50, -10, 40, 10],)
    start_date = "2024-01-01"
    end_date = "2024-01-03"

    requests = generate_requests(
        dataset,
        variable,
        pressure_level=None,
        area=area,
        start_date=start_date,
        end_date=end_date,
        saving_main_directory=tmp_path,
        saving_temporal_aggregation="daily",
    )

    assert len(requests) == 3
    assert requests[0]["catalogue_entry"] == dataset
    assert "variable" in requests[0]["request"]
    expected_variable = "maximum_2m_temperature_since_previous_post_processing"
    assert requests[0]["request"]["variable"] == expected_variable


@patch("ingestion_pipeline.ingestion.os.makedirs")
@patch("ingestion_pipeline.ingestion.os.path.exists", return_value=False)
def test_generate_filename(mock_exists, mock_makedirs, tmp_path):
    """Test the filename generation."""
    dataset = "derived-era5-single-levels-daily-statistics"
    date = datetime(2024, 1, 1)
    variable = "tasmax"
    filename = generate_filename(tmp_path, "daily", dataset, date, variable)

    expected_path = os.path.join(
        tmp_path,
        dataset,
        variable,
        "None",
        "2024",
        "tasmax_None_derived-era5-single-levels-daily-statistics_20240101.nc",
    )
    assert filename == expected_path
    mock_makedirs.assert_called_once()
