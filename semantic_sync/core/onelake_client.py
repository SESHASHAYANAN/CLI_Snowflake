"""
OneLake client for accessing Fabric Lakehouse table metadata.

This module provides access to Fabric Lakehouse-backed datasets that don't
support the standard Power BI REST API or DAX queries for table extraction.

OneLake uses Azure Data Lake Storage Gen2 APIs with the endpoint:
https://onelake.dfs.fabric.microsoft.com/
"""

from __future__ import annotations

import json
from typing import Any
import requests

from semantic_sync.config.settings import FabricConfig
from semantic_sync.auth.oauth import FabricOAuthClient
from semantic_sync.utils.logger import get_logger
from semantic_sync.utils.exceptions import ResourceNotFoundError

logger = get_logger(__name__)

# OneLake base URL
ONELAKE_BASE_URL = "https://onelake.dfs.fabric.microsoft.com"


class OneLakeClient:
    """Client for accessing OneLake (Fabric's Data Lake) for table metadata."""

    def __init__(
        self,
        config: FabricConfig,
        oauth_client: FabricOAuthClient | None = None,
    ) -> None:
        """
        Initialize OneLake client.

        Args:
            config: Fabric configuration
            oauth_client: Optional OAuth client (created if not provided)
        """
        self._config = config
        # OneLake requires storage scope
        self._oauth_client = oauth_client or FabricOAuthClient(
            config=config,
            scopes=["https://storage.azure.com/.default"]
        )
        self._workspace_id = config.workspace_id
        self._token = None

    def _get_token(self) -> str:
        """Get access token for OneLake."""
        if self._token is None:
            self._token = self._oauth_client.get_access_token()
        return self._token

    def _request(
        self,
        method: str,
        path: str,
        **kwargs,
    ) -> requests.Response:
        """Make authenticated request to OneLake."""
        headers = kwargs.pop("headers", {})
        headers["Authorization"] = f"Bearer {self._get_token()}"
        
        url = f"{ONELAKE_BASE_URL}{path}"
        response = requests.request(method, url, headers=headers, **kwargs)
        return response

    def list_items(self, workspace_name: str) -> list[dict[str, Any]]:
        """
        List items (Lakehouses, tables) in a workspace.

        Args:
            workspace_name: Name of the Fabric workspace

        Returns:
            List of items in the workspace
        """
        # OneLake path format: /{workspace_name}
        path = f"/{workspace_name}?resource=filesystem&recursive=false"
        
        try:
            response = self._request("GET", path)
            if response.status_code == 200:
                # Parse the paths response
                data = response.json()
                return data.get("paths", [])
            else:
                logger.warning(f"OneLake list failed: {response.status_code} - {response.text}")
                return []
        except Exception as e:
            logger.error(f"OneLake list error: {e}")
            return []

    def get_lakehouse_tables(
        self,
        workspace_name: str,
        lakehouse_name: str,
    ) -> list[dict[str, Any]]:
        """
        Get tables from a Fabric Lakehouse.

        Tables in Lakehouse are stored in /Tables/ directory as Delta Lake format.

        Args:
            workspace_name: Workspace name
            lakehouse_name: Lakehouse name

        Returns:
            List of table metadata
        """
        # OneLake path: /{workspace_name}/{lakehouse_name}.Lakehouse/Tables
        path = f"/{workspace_name}/{lakehouse_name}.Lakehouse/Tables?resource=filesystem&recursive=false"
        
        try:
            response = self._request("GET", path)
            if response.status_code == 200:
                data = response.json()
                paths = data.get("paths", [])
                
                tables = []
                for item in paths:
                    if item.get("isDirectory", False):
                        table_name = item.get("name", "").split("/")[-1]
                        if table_name and not table_name.startswith("_"):
                            # Get columns from Delta schema if available
                            columns = self._get_delta_table_columns(
                                workspace_name, lakehouse_name, table_name
                            )
                            tables.append({
                                "name": table_name,
                                "description": "",
                                "isHidden": False,
                                "columns": columns,
                            })
                
                logger.info(f"Retrieved {len(tables)} tables from Lakehouse via OneLake")
                return tables
            else:
                logger.warning(f"OneLake tables request failed: {response.status_code}")
                return []
        except Exception as e:
            logger.error(f"OneLake get tables error: {e}")
            return []

    def _get_delta_table_columns(
        self,
        workspace_name: str,
        lakehouse_name: str,
        table_name: str,
    ) -> list[dict[str, Any]]:
        """
        Get columns from a Delta table's schema.

        Delta tables store schema in _delta_log/*.json files.

        Args:
            workspace_name: Workspace name
            lakehouse_name: Lakehouse name
            table_name: Table name

        Returns:
            List of column definitions
        """
        # Try to read _delta_log/00000000000000000000.json for initial schema
        delta_log_path = (
            f"/{workspace_name}/{lakehouse_name}.Lakehouse/Tables/"
            f"{table_name}/_delta_log/00000000000000000000.json"
        )
        
        try:
            response = self._request("GET", delta_log_path)
            if response.status_code == 200:
                # Delta log can contain multiple JSON objects (one per line)
                for line in response.text.strip().split("\n"):
                    try:
                        entry = json.loads(line)
                        if "metaData" in entry:
                            schema = entry["metaData"].get("schemaString", "")
                            if schema:
                                schema_json = json.loads(schema)
                                fields = schema_json.get("fields", [])
                                return [
                                    {
                                        "name": f.get("name", ""),
                                        "dataType": self._map_spark_type(f.get("type", "string")),
                                        "isHidden": False,
                                        "description": f.get("metadata", {}).get("comment", ""),
                                        "isNullable": f.get("nullable", True),
                                    }
                                    for f in fields
                                ]
                    except json.JSONDecodeError:
                        continue
            
            return []
        except Exception as e:
            logger.debug(f"Could not read Delta schema for {table_name}: {e}")
            return []

    def _map_spark_type(self, spark_type: str | dict) -> str:
        """Map Spark/Delta types to Fabric semantic types."""
        if isinstance(spark_type, dict):
            # Complex type like struct, array, map
            return "Object"
        
        type_mapping = {
            "string": "String",
            "long": "Int64",
            "integer": "Int32",
            "int": "Int32",
            "short": "Int16",
            "byte": "Int8",
            "double": "Double",
            "float": "Single",
            "boolean": "Boolean",
            "date": "DateTime",
            "timestamp": "DateTime",
            "binary": "Binary",
            "decimal": "Decimal",
        }
        return type_mapping.get(str(spark_type).lower(), "String")


def get_tables_from_onelake(
    config: FabricConfig,
    workspace_name: str,
    dataset_name: str,
) -> list[dict[str, Any]]:
    """
    Convenience function to get tables from OneLake.

    Args:
        config: Fabric configuration
        workspace_name: Workspace name
        dataset_name: Dataset/Lakehouse name

    Returns:
        List of table definitions
    """
    client = OneLakeClient(config)
    return client.get_lakehouse_tables(workspace_name, dataset_name)
