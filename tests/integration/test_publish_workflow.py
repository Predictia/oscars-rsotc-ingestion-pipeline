from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from scripts.publish_to_zenodo import publish_to_zenodo


@pytest.fixture
def mock_s3_handler():
    with patch("scripts.publish_to_zenodo.S3Handler") as mock:
        handler = mock.return_value
        handler.list_files.return_value = [
            "s3://bucket/test1.zarr",
            "s3://bucket/test2.zarr",
        ]
        handler.fs = MagicMock()
        yield handler


@pytest.fixture
def mock_zenodo_client():
    with patch("scripts.publish_to_zenodo.ZenodoClient") as mock:
        client = mock.return_value
        # Mock finding existing deposition
        client.get_deposition_by_title.return_value = None
        # Mock create deposition
        client.create_deposition.return_value = {"id": 123}
        yield client


@pytest.fixture
def mock_zip_directory():
    with patch("scripts.publish_to_zenodo.zip_directory") as mock:
        yield mock


@patch("scripts.publish_to_zenodo.S3Config.from_env")
@patch("scripts.publish_to_zenodo.get_html_description")
def test_publish_to_zenodo_single_entry(
    mock_description,
    mock_s3_config,
    mock_s3_handler,
    mock_zenodo_client,
    mock_zip_directory,
):
    mock_s3_config.return_value = MagicMock()
    mock_description.return_value = "<p>Description</p>"
    runner = CliRunner()
    result = runner.invoke(
        publish_to_zenodo,
        [
            "--zenodo-token",
            "test_token",
            "--sandbox",
            "--keyword",
            "k1",
            "--keyword",
            "k2",
            "--community",
            "c1",
            "--community",
            "c2",
            "--subject",
            "s1",
            "--license",
            "MIT",
        ],
    )

    # Check if script executed successfully
    assert result.exit_code == 0
    assert "Found 2 files to sync." in result.output
    assert "Creating new deposition..." in result.output

    # Verify ZenodoClient calls
    mock_zenodo_client.get_deposition_by_title.assert_called_once()
    mock_zenodo_client.create_deposition.assert_called_once()

    # Verify metadata richness
    sent_metadata = mock_zenodo_client.create_deposition.call_args[0][0]
    assert sent_metadata["keywords"] == ["k1", "k2"]
    assert sent_metadata["communities"] == [{"identifier": "c1"}, {"identifier": "c2"}]
    assert sent_metadata["subjects"] == [{"term": "s1"}]
    assert sent_metadata["license"] == "MIT"
    assert sent_metadata["description"] == "<p>Description</p>"

    # Verify file uploads - should happen twice for the same deposition
    assert mock_zenodo_client.upload_file.call_count == 2


@patch("scripts.publish_to_zenodo.S3Config.from_env")
@patch("scripts.publish_to_zenodo.get_html_description")
def test_publish_to_zenodo_new_version(
    mock_description,
    mock_s3_config,
    mock_s3_handler,
    mock_zenodo_client,
    mock_zip_directory,
):
    mock_s3_config.return_value = MagicMock()
    mock_description.return_value = "<p>Description</p>"
    # Mock existing published deposition
    mock_zenodo_client.get_deposition_by_title.return_value = {
        "id": 123,
        "submitted": True,
    }
    # Mock new version creation returning a draft
    mock_zenodo_client.new_version.return_value = {
        "id": 456,
        "submitted": False,
        "files": [{"id": "old_file_id"}],
    }

    runner = CliRunner()
    result = runner.invoke(
        publish_to_zenodo,
        ["--zenodo-token", "test_token", "--sandbox"],
    )

    assert result.exit_code == 0
    assert "Creating new version draft..." in result.output
    assert "Cleaning up files from previous version" in result.output

    # Verify versioning calls
    mock_zenodo_client.new_version.assert_called_once_with(123)
    mock_zenodo_client.delete_file.assert_called_once_with(456, "old_file_id")

    # Verify uploads to the NEW version ID
    assert mock_zenodo_client.upload_file.call_count == 2
    for call in mock_zenodo_client.upload_file.call_args_list:
        args, kwargs = call
        assert args[0] == 456

    mock_zenodo_client.publish_deposition.assert_called_once_with(456)


@patch("scripts.publish_to_zenodo.S3Config.from_env")
@patch("scripts.publish_to_zenodo.get_html_description")
def test_publish_to_zenodo_as_draft(
    mock_description,
    mock_s3_config,
    mock_s3_handler,
    mock_zenodo_client,
    mock_zip_directory,
):
    mock_s3_config.return_value = MagicMock()
    mock_description.return_value = "<p>Description</p>"

    runner = CliRunner()
    result = runner.invoke(
        publish_to_zenodo,
        ["--zenodo-token", "test_token", "--as-draft"],
    )

    # Check if script executed successfully
    assert result.exit_code == 0
    assert "Leaving deposition ID: 123 as draft" in result.output

    # Verify ZenodoClient calls
    mock_zenodo_client.create_deposition.assert_called_once()

    # Verify publish_deposition was NOT called
    mock_zenodo_client.publish_deposition.assert_not_called()
