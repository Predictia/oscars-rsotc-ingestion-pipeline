from unittest.mock import patch

import pytest

from ingestion_pipeline.publication.main import ZenodoPublisher


@patch("ingestion_pipeline.publication.main.zipfile.is_zipfile")
@patch("ingestion_pipeline.publication.main.ZenodoClient")
@patch("ingestion_pipeline.publication.main.logger")
def test_publisher_raises_value_error_for_invalid_zip(
    mock_logger, mock_zenodo, mock_is_zipfile
):
    mock_is_zipfile.return_value = False
    mock_zenodo_instance = mock_zenodo.return_value
    mock_zenodo_instance.get_deposition_by_title.return_value = None
    mock_zenodo_instance.create_deposition.return_value = {"id": 123}

    with pytest.raises(SystemExit):
        ZenodoPublisher.run(
            provenance_crate_zip="fake.zip",
            zenodo_token="token",
            sandbox=True,
            title="title",
            keyword=[],
            community=[],
            license="MIT",
            draft=True,
        )

    mock_logger.debug.assert_any_call(
        "\nError during processing: Provided crate is not in zip format: fake.zip"
    )


@patch("ingestion_pipeline.publication.main.ZenodoClient")
@patch("ingestion_pipeline.publication.main.logger")
def test_publisher_logger_calls(mock_logger, mock_zenodo):
    mock_zenodo_instance = mock_zenodo.return_value
    # Simulate an error in get_deposition_by_title
    mock_zenodo_instance.get_deposition_by_title.side_effect = Exception("Zenodo error")

    with pytest.raises(SystemExit):
        ZenodoPublisher.run(
            provenance_crate_zip="fake.zip",
            zenodo_token="token",
            sandbox=True,
            title="title",
            keyword=[],
            community=[],
            license="MIT",
            draft=True,
        )

    # Verify logger.debug was called without err=True
    mock_logger.debug.assert_any_call(
        "Error checking Zenodo for existing deposition: Zenodo error"
    )
