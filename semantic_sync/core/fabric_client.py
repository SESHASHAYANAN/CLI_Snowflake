"""
Microsoft Fabric REST API client.

Handles authenticated communication with the Power BI / Fabric REST API
for reading and writing semantic model definitions.
"""

from __future__ import annotations


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
        retry=retry_if_exception_type((ConnectionError, RateLimitError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
    )
    def get_semantic_model_definition(self, dataset_id: str) -> dict[str, Any]:
        """
        Get semantic model definition (BIM) using Fabric V1 API.
        
        Handles Long Running Operations (LRO) with polling.
        
        Args:
            dataset_id: The dataset (item) ID
            
        Returns:
            Dictionary containing the model definition (parts, etc.)
            
        Raises:
                ResourceNotFoundError: If model not found
                ConnectionError: If API fails
        """
        # Fabric API URL
        url = f"https://api.fabric.microsoft.com/v1/workspaces/{self._config.workspace_id}/items/{dataset_id}/getDefinition"
        
        try:
            # Create ad-hoc client for Fabric scope
            fabric_oauth = FabricOAuthClient(
                self._config,
                scopes=["https://api.fabric.microsoft.com/.default"]
            )
            token = fabric_oauth.get_access_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json"
            }
            
            response = requests.post(url, headers=headers)
            
            if response.status_code == 202:
                # LRO Pattern
                import time
                location = response.headers.get("Location") or response.headers.get("Azure-AsyncOperation")
                retry_after = int(response.headers.get("Retry-After", 10))
                
                if not location:
                     raise ConnectionError(f"LRO accepted but no location header: {response.text}")
                
                logger.info(f"Model definition generation started (LRO). Polling {location}...")
                
                # Poll for completion
                start_time = time.time()
                result_location = None
                
                while time.time() - start_time < 300: # 5 min timeout
                    time.sleep(retry_after)
                    
                    poll_response = requests.get(location, headers=headers)
                    if poll_response.status_code != 200:
                        logger.warning(f"Poll gave status {poll_response.status_code}")
                        continue
                        
                    status_data = poll_response.json()
                    status = status_data.get("status")
                    
                    if status == "Succeeded":
                        logger.info("Definition generation succeeded.")
                        
                        # Check if definition is in the response
                        if "definition" in status_data:
                            return status_data
                        if "result" in status_data and "definition" in status_data.get("result", {}):
                            return status_data["result"]
                        
                        # Check for result location in headers or body
                        result_location = poll_response.headers.get("Location")
                        if not result_location:
                            # Try to get from the initial URL with /result suffix
                            result_location = location.replace("/operations/", "/operationResults/")
                        
                        break
                        
                    if status in ["Failed", "Canceled"]:
                         error = status_data.get("error", {})
                         raise ConnectionError(f"Definition retrieval failed: {error}")
                
                # Fetch the actual result if we have a location
                if result_location:
                    logger.info(f"Fetching definition result from: {result_location}")
                    result_response = requests.get(result_location, headers=headers)
                    if result_response.status_code == 200:
                        return result_response.json()
                
                # If still no result, try fetching from the getDefinition endpoint directly
                logger.info("Trying to fetch definition result directly...")
                result_url = f"https://api.fabric.microsoft.com/v1/workspaces/{self._config.workspace_id}/semanticModels/{dataset_id}/getDefinition"
                result_response = requests.get(url.replace("/getDefinition", ""), headers=headers)
                
                if result_response.status_code == 200:
                    return result_response.json()
                    
                raise ConnectionError("Could not retrieve definition result after LRO succeeded")

            elif response.status_code == 200:
                return response.json()
            
            elif response.status_code == 404:
                raise ResourceNotFoundError(f"Model {dataset_id} definition not found")
                
            else:
                self._handle_response(response)
                return {}
                
        except RequestException as e:
            raise ConnectionError(f"Failed to get definition: {e}") from e

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
        if not ds_id:
            raise ValueError("Dataset ID not provided and no default configured")
            
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
        if not ds_id:
            raise ValueError("Dataset ID not provided and no default configured")

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
        if not ds_id:
            raise ValueError("Dataset ID not provided and no default configured")

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
            Power BI Push API does NOT support POST for creating new tables
            after dataset creation. This method will:
            1. Try PUT to update existing table (works if table exists)
            2. If table doesn't exist (404), raise an error with guidance
        """
        table_name = table_definition.get('name')
        logger.info(f"Adding/updating table '{table_name}' to dataset {dataset_id}")
        
        # Try PUT first - works for existing tables
        try:
            return self.update_table(dataset_id, table_name, table_definition)
        except ResourceNotFoundError:
            # Table doesn't exist - Push API limitation
            raise ResourceNotFoundError(
                f"Cannot create new table '{table_name}' in existing Push dataset. "
                "Power BI Push API does not support adding new tables after dataset creation. "
                "Solution: Use create_push_dataset() to recreate the dataset with all required tables.",
                resource_type="push_api_table",
                details={
                    "table_name": table_name,
                    "dataset_id": dataset_id,
                    "limitation": "Push API does not support POST /tables for new tables",
                },
            )
    
    def table_exists(self, dataset_id: str, table_name: str) -> bool:
        """
        Check if a table exists in a dataset.
        
        Args:
            dataset_id: Dataset ID
            table_name: Name of the table to check
            
        Returns:
            True if table exists, False otherwise
        """
        try:
            tables = self.get_dataset_tables(dataset_id)
            return any(t.get("name") == table_name for t in tables)
        except Exception:
            return False
    
    def get_existing_table_names(self, dataset_id: str) -> set[str]:
        """
        Get the set of existing table names in a dataset.
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Set of table names
        """
        try:
            tables = self.get_dataset_tables(dataset_id)
            return {t.get("name") for t in tables if t.get("name")}
        except Exception:
            return set()
    
    def create_push_dataset(
        self,
        name: str,
        tables: list[dict[str, Any]],
        default_mode: str = "Push",
    ) -> dict[str, Any]:
        """
        Create a new Push API dataset with the specified tables.
        
        Args:
            name: Name of the new dataset
            tables: List of table definitions with columns
            default_mode: Dataset mode ("Push" or "Streaming")
            
        Returns:
            Created dataset info including new dataset ID
            
        Note:
            This creates a brand new dataset. Use this to add new tables
            since Push API doesn't support adding tables after creation.
        """
        endpoint = f"/groups/{self._config.workspace_id}/datasets"
        
        dataset_definition = {
            "name": name,
            "defaultMode": default_mode,
            "tables": tables,
        }
        
        logger.info(f"Creating Push dataset '{name}' with {len(tables)} tables")
        return self.post(endpoint, data=dataset_definition)
    
    def delete_dataset(self, dataset_id: str) -> dict[str, Any]:
        """
        Delete a dataset.
        
        Args:
            dataset_id: Dataset ID to delete
            
        Returns:
            Empty response on success
        """
        endpoint = f"/groups/{self._config.workspace_id}/datasets/{dataset_id}"
        logger.warning(f"Deleting dataset {dataset_id}")
        return self.delete(endpoint)

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
