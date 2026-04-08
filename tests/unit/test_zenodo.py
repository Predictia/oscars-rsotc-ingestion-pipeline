from unittest.mock import patch

import pytest

from ingestion_pipeline.publication.zenodo import ZenodoClient


@pytest.fixture
def zenodo_client():
    return ZenodoClient(token="test_token", sandbox=True)


def test_init(zenodo_client):
    assert zenodo_client.token == "test_token"
    assert "access_token" in zenodo_client.session.params
    assert zenodo_client.session.params["access_token"] == "test_token"
    assert "sandbox" in zenodo_client.base_url


@patch("requests.Session.post")
def test_create_deposition(mock_post, zenodo_client):
    mock_post.return_value.status_code = 201
    mock_post.return_value.json.return_value = {
        "id": 123,
        "metadata": {"title": "Test"},
    }

    metadata = {"title": "Test", "description": "Desc"}
    res = zenodo_client.create_deposition(metadata)

    assert res["id"] == 123
    mock_post.assert_called_once()


@patch("requests.Session.get")
def test_get_deposition(mock_get, zenodo_client):
    mock_get.return_value.status_code = 200
    mock_get.return_value.json.return_value = {"id": 123}

    res = zenodo_client.get_deposition(123)
    assert res["id"] == 123


@patch("requests.Session.get")
@patch("requests.Session.put")
def test_upload_file(mock_put, mock_get, zenodo_client, tmp_path):
    mock_get.return_value.json.return_value = {"links": {"bucket": "http://bucket"}}
    mock_put.return_value.status_code = 200
    mock_put.return_value.json.return_value = {"filename": "test.txt"}

    test_file = tmp_path / "test.txt"
    test_file.write_text("hello")

    res = zenodo_client.upload_file(123, test_file)
    assert res["filename"] == "test.txt"
    mock_put.assert_called_once()


@patch("requests.Session.post")
@patch("requests.Session.get")
def test_new_version(mock_get, mock_post, zenodo_client):
    # Mock create new version action
    mock_post.return_value.json.return_value = {
        "links": {"latest_draft": "http://api/deposit/depositions/456"}
    }
    # Mock getting the new version draft details
    mock_get.return_value.json.return_value = {"id": 456}

    res = zenodo_client.new_version(123)
    assert res["id"] == 456


@patch("requests.Session.get")
def test_get_deposition_by_title(mock_get, zenodo_client):
    mock_get.return_value.json.return_value = [
        {"metadata": {"title": "Test Title"}, "id": 1},
        {"metadata": {"title": "Other Title"}, "id": 2},
    ]

    res = zenodo_client.get_deposition_by_title("Test Title")
    assert res["id"] == 1

    res = zenodo_client.get_deposition_by_title("Non Existent")
    assert res is None
