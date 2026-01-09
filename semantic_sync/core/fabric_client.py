"""
Microsoft Fabric REST API client.

Handles authenticated communication with the Power BI / Fabric REST API
for reading and writing semantic model definitions.
"""

from typing import Any

import requests
from requests.exceptions import HTTPError, RequestException
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

from semantic_sync.auth.oauth import FabricOAuthClient, get_oauth_client
from semantic_sync.config.settings import FabricConfig
from semantic_sync.utils.exceptions import (
    AuthenticationError,
    ConnectionError,
    RateLimitError,
    ResourceNotFoundError,
)
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class FabricClient:
    """
    REST API client for Microsoft Fabric / Power BI.

    Handles authenticated requests with automatic token refresh on 401 errors.
    """

    def __init__(
        self,
        config: FabricConfig,
        oauth_client: FabricOAuthClient | None = None,
    ) -> None:
        """
        Initialize Fabric client.

        Args:
            config: Fabric configuration
            oauth_client: Optional OAuth client (created if not provided)
        """
        self._config = config
        self._oauth = oauth_client or get_oauth_client(config)
        self._base_url = config.api_base_url
        self._session = requests.Session()

    def _get_headers(self, force_refresh: bool = False) -> dict[str, str]:
        """Get request headers with valid auth token."""
        auth_headers = self._oauth.get_authorization_header(force_refresh=force_refresh)
        return {
            **auth_headers,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _handle_response(self, response: requests.Response) -> dict[str, Any]:
        """
        Handle API response, raising appropriate exceptions on errors.

        Args:
            response: Response from API call

        Returns:
            Parsed JSON response

        Raises:
            AuthenticationError: On 401/403
            RateLimitError: On 429
            ResourceNotFoundError: On 404
            ConnectionError: On other errors
        """
        if response.status_code == 401:
            raise AuthenticationError(
                "Fabric API authentication failed",
                provider="Fabric API",
                details={"status_code": 401},
            )

        if response.status_code == 403:
            raise AuthenticationError(
                "Fabric API access forbidden - check permissions",
                provider="Fabric API",
                details={"status_code": 403},
            )

        if response.status_code == 404:
            raise ResourceNotFoundError(
                "Resource not found in Fabric",
                resource_type="fabric_resource",
                details={"status_code": 404, "url": response.url},
            )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 60))
            raise RateLimitError(
                "Fabric API rate limit exceeded",
                retry_after=retry_after,
            )

        try:
            response.raise_for_status()
        except HTTPError as e:
            raise ConnectionError(
                f"Fabric API request failed: {e}",
                service="Fabric API",
                details={
                    "status_code": response.status_code,
                    "response": response.text[:500] if response.text else None,
                },
            ) from e

        if response.status_code == 204:
            return {}

        try:
            return response.json()
        except ValueError:
            return {"raw_response": response.text}

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(AuthenticationError),
        before_sleep=lambda retry_state: logger.info("Retrying after auth refresh..."),
    )
    def _request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        retry_on_401: bool = True,
    ) -> dict[str, Any]:
        """
        Make authenticated request to Fabric API.

        Args:
            method: HTTP method
            endpoint: API endpoint (relative to base URL)
            data: Request body (JSON)
            params: Query parameters
            retry_on_401: If True, refresh token and retry on 401

        Returns:
            Parsed JSON response
        """
        url = f"{self._base_url}{endpoint}"
        logger.debug(f"API request: {method} {url}")

        try:
            response = self._session.request(
                method=method,
                url=url,
                headers=self._get_headers(),
                json=data,
                params=params,
                timeout=30,
            )

            return self._handle_response(response)

        except AuthenticationError:
            if retry_on_401:
                # Refresh token and retry once
                logger.info("Refreshing OAuth token after 401")
                self._oauth.get_access_token(force_refresh=True)
                response = self._session.request(
                    method=method,
                    url=url,
                    headers=self._get_headers(force_refresh=True),
                    json=data,
                    params=params,
                    timeout=30,
                )
                return self._handle_response(response)
            raise

        except RequestException as e:
            raise ConnectionError(
                f"Failed to connect to Fabric API: {e}",
                service="Fabric API",
            ) from e

    def get(
        self,
        endpoint: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Make GET request."""
        return self._request("GET", endpoint, params=params)

    def post(
        self,
        endpoint: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Make POST request."""
        return self._request("POST", endpoint, data=data)

    def patch(
        self,
        endpoint: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Make PATCH request."""
        return self._request("PATCH", endpoint, data=data)

    def delete(self, endpoint: str) -> dict[str, Any]:
        """Make DELETE request."""
        return self._request("DELETE", endpoint)

    # ----------------------------------------------------------------
    # Dataset / Semantic Model Operations
    # ----------------------------------------------------------------

    def get_dataset(self, dataset_id: str | None = None) -> dict[str, Any]:
        """
        Get dataset (semantic model) details.

        Args:
            dataset_id: Dataset ID (uses configured ID if not provided)

        Returns:
            Dataset metadata
        """
        ds_id = dataset_id or self._config.dataset_id
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{ds_id}"
        return self.get(endpoint)

    def get_dataset_tables(self, dataset_id: str | None = None) -> list[dict[str, Any]]:
        """
        Get all tables in a dataset.

        Args:
            dataset_id: Dataset ID

        Returns:
            List of table definitions
        """
        ds_id = dataset_id or self._config.dataset_id
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{ds_id}/tables"
        response = self.get(endpoint)
        return response.get("value", [])

    def get_dataset_refresh_history(
        self,
        dataset_id: str | None = None,
        top: int = 10,
    ) -> list[dict[str, Any]]:
        """
        Get dataset refresh history.

        Args:
            dataset_id: Dataset ID
            top: Number of records to return

        Returns:
            List of refresh history records
        """
        ds_id = dataset_id or self._config.dataset_id
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{ds_id}/refreshes"
        response = self.get(endpoint, params={"$top": top})
        return response.get("value", [])

    def trigger_dataset_refresh(self, dataset_id: str | None = None) -> dict[str, Any]:
        """
        Trigger a dataset refresh.

        Args:
            dataset_id: Dataset ID

        Returns:
            Refresh response
        """
        ds_id = dataset_id or self._config.dataset_id
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{ds_id}/refreshes"
        return self.post(endpoint, data={"notifyOption": "NoNotification"})

    def update_dataset_tables(
        self,
        tables: list[dict[str, Any]],
        dataset_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Update table definitions in a dataset.

        Args:
            tables: List of table definitions to update
            dataset_id: Dataset ID

        Returns:
            Update response
        """
        ds_id = dataset_id or self._config.dataset_id
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{ds_id}/tables"
        return self.post(endpoint, data={"tables": tables})

    def update_table(
        self,
        dataset_id: str,
        table_name: str,
        table_definition: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Update a single table definition in a Push dataset.
        
        Args:
            dataset_id: Dataset ID
            table_name: Name of the table to update
            table_definition: New table definition
            
        Returns:
            Update response
        """
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{dataset_id}/tables/{table_name}"
        logger.info(f"Updating table '{table_name}' in dataset {dataset_id}")
        return self.put(endpoint, data=table_definition)

    def add_table(
        self,
        dataset_id: str,
        table_definition: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Add a new table to a Push dataset.

        Args:
            dataset_id: Dataset ID
            table_definition: Table definition with name and columns

        Returns:
            Response from API

        Note:
            Only works with Push API datasets (addRowsAPIEnabled=true).
            For Import datasets, use XMLA/TOM API instead.
        """
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{dataset_id}/tables"
        logger.info(f"Adding table '{table_definition.get('name')}' to dataset {dataset_id}")
        # Use POST to create new table (PUT is for updates)
        return self.post(endpoint, data=table_definition)
    
    def put(
        self,
        endpoint: str,
        data: dict[str, Any],
    ) -> dict[str, Any]:
        """Make PUT request."""
        return self._request("PUT", endpoint, data=data)

    # ----------------------------------------------------------------
    # XMLA / Enhanced Model Operations (requires Premium capacity)
    # ----------------------------------------------------------------

    def execute_queries(
        self,
        dataset_id: str | None = None,
        queries: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """
        Execute DAX queries against a dataset.

        Args:
            dataset_id: Dataset ID
            queries: List of DAX queries to execute

        Returns:
            Query results
        """
        ds_id = dataset_id or self._config.dataset_id
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{ds_id}/executeQueries"

        if not queries:
            queries = [{"query": "EVALUATE ROW(\"test\", 1)"}]

        return self.post(endpoint, data={
            "queries": queries,
            "serializerSettings": {"includeNulls": True},
        })

    # ----------------------------------------------------------------
    # Workspace Operations
    # ----------------------------------------------------------------

    def get_workspace(self, workspace_id: str | None = None) -> dict[str, Any]:
        """
        Get workspace details.

        Args:
            workspace_id: Workspace ID

        Returns:
            Workspace metadata
        """
        ws_id = workspace_id or self._config.workspace_id
        return self.get(f"/groups/{ws_id}")

    def list_workspace_datasets(
        self,
        workspace_id: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        List all datasets in a workspace.

        Args:
            workspace_id: Workspace ID

        Returns:
            List of datasets
        """
        ws_id = workspace_id or self._config.workspace_id
        response = self.get(f"/groups/{ws_id}/datasets")
        return response.get("value", [])

    # ----------------------------------------------------------------
    # Validation
    # ----------------------------------------------------------------

    def validate_connection(self) -> bool:
        """
        Validate Fabric API connectivity.

        Returns:
            True if connection and authentication are valid

        Raises:
            AuthenticationError: If authentication fails
            ConnectionError: If API is unreachable
        """
        try:
            # Try to get workspace info as validation
            self.get_workspace()
            logger.info("Fabric API connection validated successfully")
            return True
        except (AuthenticationError, ConnectionError):
            raise
        except Exception as e:
            raise ConnectionError(
                f"Failed to validate Fabric connection: {e}",
                service="Fabric API",
            ) from e
