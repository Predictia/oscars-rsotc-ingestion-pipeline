from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import xarray as xr

from ingestion_pipeline.utilities.netcdf_utils import load_netcdf, write_netcdf


@patch("ingestion_pipeline.utilities.netcdf_utils.xr.open_dataset")
def test_load_netcdf_success(mock_open):
    mock_ds = MagicMock(spec=xr.Dataset)
    mock_open.return_value = mock_ds

    path = Path("test.nc")
    ds = load_netcdf(path)

    assert ds == mock_ds
    mock_open.assert_called_once_with(path)


@patch("ingestion_pipeline.utilities.netcdf_utils.xr.open_dataset")
def test_load_netcdf_failure(mock_open):
    mock_open.side_effect = IOError("File not found")

    with pytest.raises(IOError):
        load_netcdf(Path("missing.nc"))


def test_write_netcdf():
    # Mock dataset
    mock_ds = MagicMock(spec=xr.Dataset)
    mock_ds.data_vars = ["var1", "var2"]

    path = Path("output.nc")

    result_path = write_netcdf(mock_ds, path)

    assert result_path == path
    mock_ds.to_netcdf.assert_called_once()

    # Check encoding was passed
    args, kwargs = mock_ds.to_netcdf.call_args
    assert "encoding" in kwargs
    encoding = kwargs["encoding"]
    assert "var1" in encoding
    assert encoding["var1"]["zlib"] is True
