import logging
from pathlib import Path
from typing import Any, Dict, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class ZenodoClient:
    """Client for interacting with the Zenodo REST API."""

    def __init__(self, token: str, sandbox: bool = True):
        self.token = token
        self.base_url = (
            "https://sandbox.zenodo.org/api" if sandbox else "https://zenodo.org/api"
        )
        self.session = requests.Session()
        self.session.params = {"access_token": self.token}

        # Configure retries
        retry_strategy = Retry(
            total=5,
            backoff_factor=2,  # Exponential backoff: 2, 4, 8, 16, 32 seconds
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "PUT"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)

    def create_deposition(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new deposition.

        Metadata should include:
        - title: str
        - upload_type: str (e.g., 'dataset')
        - description: str
        - creators: list of dict (e.g., [{'name': '...', 'affiliation': '...'}])
        - keywords: list of str (optional)
        - communities: list of dict (e.g., [{'identifier': '...'}]) (optional)
        - license: str (slug, e.g., 'cc-by-4.0') (optional)
        """
        url = f"{self.base_url}/deposit/depositions"
        headers = {"Content-Type": "application/json"}
        response = self.session.post(url, json={"metadata": metadata}, headers=headers)
        response.raise_for_status()
        return response.json()

    def get_deposition(self, deposition_id: int) -> Dict[str, Any]:
        """Get a deposition by ID."""
        url = f"{self.base_url}/deposit/depositions/{deposition_id}"
        response = self.session.get(url)
        response.raise_for_status()
        return response.json()

    def update_deposition_metadata(
        self, deposition_id: int, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update deposition metadata."""
        url = f"{self.base_url}/deposit/depositions/{deposition_id}"
        headers = {"Content-Type": "application/json"}
        response = self.session.put(url, json={"metadata": metadata}, headers=headers)
        response.raise_for_status()
        return response.json()

    def upload_file(self, deposition_id: int, file_path: Path) -> Dict[str, Any]:
        """Upload a file to a deposition."""
        deposition = self.get_deposition(deposition_id)
        bucket_url = deposition["links"]["bucket"]

        url = f"{bucket_url}/{file_path.name}"
        with open(file_path, "rb") as f:
            response = self.session.put(url, data=f)
        response.raise_for_status()
        return response.json()

    def delete_file(self, deposition_id: int, file_id: str) -> None:
        """Delete a file from a deposition."""
        url = f"{self.base_url}/deposit/depositions/{deposition_id}/files/{file_id}"
        response = self.session.delete(url)
        response.raise_for_status()

    def publish_deposition(self, deposition_id: int) -> Dict[str, Any]:
        """Publish a deposition."""
        url = f"{self.base_url}/deposit/depositions/{deposition_id}/actions/publish"
        response = self.session.post(url)
        response.raise_for_status()
        return response.json()

    def new_version(self, deposition_id: int) -> Dict[str, Any]:
        """Create a new version of a published deposition."""
        url = f"{self.base_url}/deposit/depositions/{deposition_id}/actions/newversion"
        response = self.session.post(url)
        response.raise_for_status()
        # This returns the new version draft
        new_version_draft = response.json()
        # The URL for the new version draft is in the links
        new_deposition_id = int(
            new_version_draft["links"]["latest_draft"].split("/")[-1]
        )
        return self.get_deposition(new_deposition_id)

    def get_depositions(self, q: Optional[str] = None) -> list[Dict[str, Any]]:
        """List depositions, optionally filtered by query."""
        url = f"{self.base_url}/deposit/depositions"
        params = {}
        if q:
            params["q"] = q
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def get_deposition_by_title(self, title: str) -> Optional[Dict[str, Any]]:
        """Find a deposition by its exact title."""
        depositions = self.get_depositions(q=f'title:"{title}"')
        for d in depositions:
            if d["metadata"]["title"] == title:
                return d
        return None
