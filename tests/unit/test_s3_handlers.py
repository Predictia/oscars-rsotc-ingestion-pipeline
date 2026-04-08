import os
from unittest.mock import MagicMock, patch

import pytest
import xarray as xr

from ingestion_pipeline.utilities.s3_handlers import (
    S3Config,
    S3Handler,
)


@pytest.fixture
def s3_config():
    return S3Config(
        bucket_name="test-bucket",
        endpoint_url="https://s3.example.com",
        access_key="acc",
        secret_key="sec",
        region="test-region",
    )


def test_s3_config_from_env():
    # Use patch.dict to set environment variables
    with patch.dict(
        os.environ,
        {
            "S3_BUCKET_NAME": "env-bucket",
            "S3_ENDPOINT_URL": "https://env.s3.com",
            "S3_ACCESS_KEY": "env-acc",
            "S3_SECRET_KEY": "env-sec",
            "S3_REGION": "env-region",
        },
    ):
        config = S3Config.from_env()
        assert config.bucket_name == "env-bucket"
        assert config.endpoint_url == "https://env.s3.com"


def test_s3_config_from_env_missing_vars():
    # Ensure no environment variables interfere
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ValueError, match="Missing required environment variables"):
            S3Config.from_env()


@patch("ingestion_pipeline.utilities.s3_handlers.fsspec.filesystem")
def test_s3_handler_init(mock_fs, s3_config):
    handler = S3Handler(s3_config)
    assert handler.s3_config == s3_config
    mock_fs.assert_called_with(
        "s3",
        key="acc",
        secret="sec",
        client_kwargs={
            "endpoint_url": "https://s3.example.com",
            "region_name": "test-region",
        },
        config_kwargs={"max_pool_connections": 50},
    )
    assert handler.base_path == "s3://test-bucket/"
    assert handler.get_s3_path("file.nc") == "s3://test-bucket/file.nc"
    assert handler.path_exists("s3://test-bucket/") is not None  # mock returns mock


@patch("ingestion_pipeline.utilities.s3_handlers.fsspec.filesystem")
def test_list_files(mock_fs, s3_config):
    mock_fs_instance = mock_fs.return_value
    mock_fs_instance.ls.return_value = [
        "test-bucket/file1.nc",
        "test-bucket/file2.zarr",
    ]

    handler = S3Handler(s3_config)
    files = handler.list_files(suffix=".nc")

    assert len(files) == 1
    assert files[0] == "s3://test-bucket/file1.nc"

    # Test pattern filtering
    mock_fs_instance.ls.return_value = ["b/file1.nc", "b/other.nc"]
    # Should use fnmatch on the name
    files_pat = handler.list_files(pattern="file1*")
    assert len(files_pat) == 1
    assert "file1.nc" in files_pat[0]


@patch("ingestion_pipeline.utilities.s3_handlers.xr.open_zarr")
@patch("ingestion_pipeline.utilities.s3_handlers.fsspec.filesystem")
def test_read_file(mock_fs, mock_open_zarr, s3_config):
    handler = S3Handler(s3_config)

    mock_ds = MagicMock(spec=xr.Dataset)
    mock_ds.__contains__.side_effect = lambda key: key == "var1"
    mock_open_zarr.return_value = mock_ds

    # Happy path
    mock_ds.__getitem__.return_value = mock_ds  # simulate ds[[variable]]
    ds = handler.read_file("s3://bucket/test.zarr", variable="var1")
    assert ds is not None

    # Variable missing
    mock_ds.__contains__.side_effect = lambda key: False
    with pytest.raises(ValueError, match="Variable 'missing' not found"):
        handler.read_file("s3://bucket/test.zarr", variable="missing")


@patch("ingestion_pipeline.utilities.s3_handlers.fsspec.filesystem")
def test_check_zarr_exists(mock_fs, s3_config):
    handler = S3Handler(s3_config)
    mock_fs_instance = mock_fs.return_value

    # Case 1: .zmetadata exists
    mock_fs_instance.exists.side_effect = lambda path: path.endswith(".zmetadata")
    exists = handler.check_zarr_exists("key")
    assert exists is True

    # Case 2: Not consolidated, check for .zgroup and .zarray
    mock_fs_instance.exists.side_effect = lambda path: path.endswith(".zgroup")
    mock_fs_instance.find.return_value = ["test-bucket/key/var/.zarray"]

    exists_unconsolidated = handler.check_zarr_exists("key")
    assert exists_unconsolidated is True

    # Case 3: Nothing
    mock_fs_instance.exists.side_effect = None
    mock_fs_instance.exists.return_value = False
    exists_none = handler.check_zarr_exists("key")
    assert exists_none is False


@patch("ingestion_pipeline.utilities.s3_handlers.fsspec.get_mapper")
def test_write_ds_success(mock_mapper, s3_config):
    handler = S3Handler(s3_config)
    ds = MagicMock(spec=xr.Dataset)

    # Success write
    success = handler.write_ds(ds, "output.zarr", overwrite=True)
    assert success is True
    ds.to_zarr.assert_called()

    # Success append
    success = handler.write_ds(ds, "output.zarr", append_dim="time")
    assert success is True
    call_kwargs = ds.to_zarr.call_args[1]
    assert call_kwargs["mode"] == "a"
    assert call_kwargs["append_dim"] == "time"


@patch.object(S3Handler, "check_zarr_exists")
def test_write_ds_skip_existing(mock_check, s3_config):
    # Mock existence = True
    mock_check.return_value = True
    handler = S3Handler(s3_config)
    ds = MagicMock(spec=xr.Dataset)

    # Should return True early without calling to_zarr
    success = handler.write_ds(ds, "existing.zarr", overwrite=False)
    assert success is True
    ds.to_zarr.assert_not_called()


@patch("ingestion_pipeline.utilities.s3_handlers.fsspec.get_mapper")
def test_write_ds_failure(mock_mapper, s3_config):
    handler = S3Handler(s3_config)
    ds = MagicMock(spec=xr.Dataset)
    ds.to_zarr.side_effect = RuntimeError("S3 error")

    # Should return False
    success = handler.write_ds(ds, "fail.zarr", overwrite=True)
    assert success is False


@patch("ingestion_pipeline.utilities.s3_handlers.boto3.client")
def test_upload_file(mock_boto, s3_config):
    handler = S3Handler(s3_config)
    handler.upload_file("local.txt", "remote.txt")

    mock_boto.return_value.upload_file.assert_called_with(
        "local.txt", "test-bucket", "remote.txt"
    )
