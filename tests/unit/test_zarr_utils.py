from unittest.mock import MagicMock, patch

import pytest

from ingestion_pipeline.utilities.zarr_utils import convert_netcdfs_to_zarr


@patch("ingestion_pipeline.utilities.zarr_utils.xr.open_mfdataset")
@patch("ingestion_pipeline.utilities.zarr_utils.os.remove")
def test_convert_netcdfs_to_zarr(mock_remove, mock_open_mf, tmp_path):
    # Mock dataset
    mock_ds = MagicMock()
    mock_ds.chunk.return_value = mock_ds
    mock_open_mf.return_value = mock_ds

    input_files = ["file1.nc", "file2.nc"]
    output_zarr = str(tmp_path / "output.zarr")

    # Test without cleaning
    res = convert_netcdfs_to_zarr(input_files, output_zarr, clean_input_files=False)
    assert res == output_zarr
    mock_ds.to_zarr.assert_called_once_with(output_zarr, mode="w", consolidated=True)
    mock_remove.assert_not_called()

    # Test with cleaning
    mock_ds.reset_mock()
    res = convert_netcdfs_to_zarr(input_files, output_zarr, clean_input_files=True)
    assert mock_remove.call_count == 2
    mock_remove.assert_any_call("file1.nc")
    mock_remove.assert_any_call("file2.nc")


@patch("ingestion_pipeline.utilities.zarr_utils.xr.open_mfdataset")
def test_convert_netcdfs_to_zarr_failure(mock_open_mf):
    mock_open_mf.side_effect = Exception("Open failed")

    with pytest.raises(Exception, match="Open failed"):
        convert_netcdfs_to_zarr(["file1.nc"], "out.zarr")
