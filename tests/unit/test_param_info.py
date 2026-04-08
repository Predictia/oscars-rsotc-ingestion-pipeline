from unittest.mock import MagicMock, patch

import xarray

from ingestion_pipeline.utilities.param_info import ParamInfo, get_param_information


def test_param_info_initialization():
    """Test the initialization of the ParamInfo class."""
    param = ParamInfo(
        param_id=1,
        long_name="Temperature",
        short_name="Temp",
        units="K",
        description="Air temperature at 2 meters",
    )
    assert param.param_id == 1
    assert param.short_name == "Temp"
    assert param.long_name == "Temperature"
    assert param.units == "K"
    assert param.description == "Air temperature at 2 meters"


@patch("ingestion_pipeline.utilities.param_info.requests.get")
def test_get_param_information(mock_get):
    """Test the get_param_information function with mocked data."""
    # Mock dataset
    data = xarray.Dataset({"temperature": ([1.0], [1.0], {"GRIB_paramId": 130})})

    # Mock API responses
    mock_param_response = {
        "results": [
            {
                "id": 130,
                "name": "Temperature",
                "shortname": "Temp",
                "unit_id": 1,
                "description": "Air temperature at 2 meters",
            }
        ]
    }
    mock_unit_response = [{"id": 1, "name": "K"}]

    mock_get.side_effect = [
        MagicMock(json=lambda: mock_param_response),
        MagicMock(json=lambda: mock_unit_response),
    ]

    # Call the function
    param_info = get_param_information(data)["temperature"]

    # Assertions
    assert param_info is not None
    assert param_info.param_id == 130
    assert param_info.long_name == "Temperature"
    assert param_info.short_name == "Temp"
    assert param_info.units == "K"
    assert param_info.description == "Air temperature at 2 meters"


@patch("ingestion_pipeline.utilities.param_info.requests.get")
def test_get_param_information_missing_param_id(mock_get):
    """Test get_param_information when GRIB_paramId is missing."""
    data = xarray.Dataset({"temperature": ([0.1], [1.0], {})})

    param_info = get_param_information(data)

    assert param_info is None
