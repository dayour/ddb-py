"""HTTP client wrapper for Darango API."""

from typing import Any, Optional, cast

import httpx

from ddb.config import get_darango_api


class DarangoError(Exception):
    """Exception raised for Darango API errors."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)


class DarangoClient:
    """HTTP client for interacting with the Darango API."""

    def __init__(self, base_url: Optional[str] = None, timeout: float = 30.0):
        """Initialize the Darango client.

        Args:
            base_url: The base URL of the Darango API. Defaults to DARANGO_API env var.
            timeout: Request timeout in seconds.
        """
        self.base_url = (base_url or get_darango_api()).rstrip("/")
        self.timeout = timeout

    def _request(
        self,
        method: str,
        path: str,
        json: Optional[dict[str, Any]] = None,
        params: Optional[dict[str, Any]] = None,
    ) -> Any:
        """Make an HTTP request to the Darango API.

        Args:
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            path: API path
            json: JSON body for the request
            params: Query parameters

        Returns:
            JSON response from the API

        Raises:
            DarangoError: If the request fails
        """
        url = f"{self.base_url}{path}"

        try:
            response = httpx.request(
                method, url, json=json, params=params, timeout=self.timeout
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            error_msg = str(e)
            try:
                error_data = e.response.json()
                if "errorMessage" in error_data:
                    error_msg = error_data["errorMessage"]
                elif "error" in error_data:
                    error_msg = error_data["error"]
            except Exception:
                pass
            raise DarangoError(error_msg, e.response.status_code) from e
        except httpx.RequestError as e:
            raise DarangoError(f"Request failed: {e}") from e

    def create_collection(
        self, db: str, name: str, collection_type: str = "document"
    ) -> dict[str, Any]:
        """Create a new collection.

        Args:
            db: Database name
            name: Collection name
            collection_type: Type of collection ("document" or "edge")

        Returns:
            API response
        """
        # Map type to ArangoDB type code (2 = document, 3 = edge)
        type_code = 3 if collection_type == "edge" else 2
        return cast(
            dict[str, Any],
            self._request(
                "POST",
                f"/_db/{db}/_api/collection",
                json={"name": name, "type": type_code},
            ),
        )

    def query(
        self, db: str, aql: str, bind_vars: Optional[dict[str, Any]] = None
    ) -> dict[str, Any]:
        """Execute an AQL query.

        Args:
            db: Database name
            aql: AQL query string
            bind_vars: Bind variables for the query

        Returns:
            Query result
        """
        body: dict[str, Any] = {"query": aql}
        if bind_vars:
            body["bindVars"] = bind_vars
        return cast(
            dict[str, Any],
            self._request("POST", f"/_db/{db}/_api/cursor", json=body),
        )

    def get_document(self, db: str, collection: str, key: str) -> dict[str, Any]:
        """Get a document by key.

        Args:
            db: Database name
            collection: Collection name
            key: Document key

        Returns:
            Document data
        """
        return cast(
            dict[str, Any],
            self._request("GET", f"/_db/{db}/_api/document/{collection}/{key}"),
        )

    def insert_document(
        self, db: str, collection: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        """Insert a new document.

        Args:
            db: Database name
            collection: Collection name
            document: Document data

        Returns:
            Insert result
        """
        return cast(
            dict[str, Any],
            self._request(
                "POST", f"/_db/{db}/_api/document/{collection}", json=document
            ),
        )

    def update_document(
        self, db: str, collection: str, key: str, document: dict[str, Any]
    ) -> dict[str, Any]:
        """Update an existing document.

        Args:
            db: Database name
            collection: Collection name
            key: Document key
            document: Updated document data

        Returns:
            Update result
        """
        return cast(
            dict[str, Any],
            self._request(
                "PATCH", f"/_db/{db}/_api/document/{collection}/{key}", json=document
            ),
        )

    def delete_document(self, db: str, collection: str, key: str) -> dict[str, Any]:
        """Delete a document.

        Args:
            db: Database name
            collection: Collection name
            key: Document key

        Returns:
            Delete result
        """
        return cast(
            dict[str, Any],
            self._request("DELETE", f"/_db/{db}/_api/document/{collection}/{key}"),
        )
