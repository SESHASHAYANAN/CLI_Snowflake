"""
XMLA-based reader for Fabric/Power BI semantic models.

This module provides XMLA endpoint access for Import datasets that don't 
support the Push API /tables REST endpoint.
"""

from typing import Any
import requests

from semantic_sync.config.settings import FabricConfig
from semantic_sync.auth.oauth import FabricOAuthClient
from semantic_sync.utils.logger import get_logger
from semantic_sync.utils.exceptions import ResourceNotFoundError

logger = get_logger(__name__)


class FabricXmlaClient:
    """XMLA client for reading Fabric/Power BI semantic models via REST API."""

    def __init__(self, config: FabricConfig, oauth_client: FabricOAuthClient | None = None) -> None:
        """
        Initialize XMLA client.

        Args:
            config: Fabric configuration with credentials
            oauth_client: Optional OAuth client (created if not provided)
        """
        self._config = config
        self._oauth_client = oauth_client or FabricOAuthClient(config=config)
        self._workspace_id = config.workspace_id
        self._dataset_id = config.dataset_id
        self._token = None

    def connect(self, workspace_name: str, dataset_name: str) -> None:
        """
        Connect to dataset via XMLA endpoint.

        Args:
            workspace_name: Name of the workspace (not used, kept for API compatibility)
            dataset_name: Name of the dataset (not used, kept for API compatibility)
        """
        # Get access token
        self._token = self._oauth_client.get_access_token()
        logger.info("XMLA client initialized successfully")

    def _execute_dmv_query(self, dmv_query: str) -> list[dict[str, Any]]:
        """
        Execute a DMV (Dynamic Management Views) query.

        Args:
            dmv_query: DMV query to execute

        Returns:
            List of rows from the query result

        Raises:
            ResourceNotFoundError: If query execution fails
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self._workspace_id}/datasets/{self._dataset_id}/executeQueries"
        
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "queries": [{
                "query": dmv_query
            }],
            "serializerSettings": {
                "includeNulls": False
            }
        }
        
        try:
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                rows = result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                return rows
            else:
                error_msg = f"DMV query failed with status {response.status_code}: {response.text}"
                logger.error(error_msg)
                raise ResourceNotFoundError(error_msg, resource_type="dmv_query")
                
        except requests.RequestException as e:
            error_msg = f"Failed to execute DMV query: {e}"
            logger.error(error_msg)
            raise ResourceNotFoundError(error_msg, resource_type="dmv_query")

    def get_tables(self) -> list[dict[str, Any]]:
        """
        Get all tables from the model using DMV queries.

        Returns:
            List of table definitions
        """
        try:
            # Query for tables
            dmv_query = "SELECT [Name], [Description], [IsHidden] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [ObjectType] = 'Table'"
            tables_rows = self._execute_dmv_query(dmv_query)
            
            tables = []
            for table_row in tables_rows:
                table_name = table_row.get("[Name]", "")
                if not table_name:
                    continue
                
                # Get columns for this table
                columns_query = f"SELECT [Name], [DataType], [IsHidden], [Description] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [TableName] = '{table_name}'"
                columns_rows = self._execute_dmv_query(columns_query)
                
                columns = []
                for col_row in columns_rows:
                    col_name = col_row.get("[Name]", "")
                    if col_name:
                        columns.append({
                            "name": col_name,
                            "dataType": col_row.get("[DataType]", "String"),
                            "isHidden": col_row.get("[IsHidden]", False),
                            "description": col_row.get("[Description]", ""),
                            "isNullable": True  # Default, not always available in DMV
                        })
                
                tables.append({
                    "name": table_name,
                    "description": table_row.get("[Description]", ""),
                    "isHidden": table_row.get("[IsHidden]", False),
                    "columns": columns
                })
            
            logger.info(f"Retrieved {len(tables)} tables via DMV")
            return tables
            
        except Exception as e:
            logger.error(f"Failed to retrieve tables via DMV: {e}")
            # Return empty list instead of raising to allow graceful degradation
            return []

    def get_measures(self) -> list[dict[str, Any]]:
        """
        Get all measures from the model using DMV queries.

        Returns:
            List of measure definitions
        """
        try:
            dmv_query = "SELECT [Name], [Expression], [Description], [IsHidden], [TableName] FROM $SYSTEM.TMSCHEMA_MEASURES"
            measures_rows = self._execute_dmv_query(dmv_query)
            
            measures = []
            for measure_row in measures_rows:
                measure_name = measure_row.get("[Name]", "")
                if measure_name:
                    measures.append({
                        "name": measure_name,
                        "expression": measure_row.get("[Expression]", ""),
                        "description": measure_row.get("[Description]", ""),
                        "isHidden": measure_row.get("[IsHidden]", False),
                        "table": measure_row.get("[TableName]", "")
                    })
            
            logger.info(f"Retrieved {len(measures)} measures via DMV")
            return measures
            
        except Exception as e:
            logger.error(f"Failed to retrieve measures via DMV: {e}")
            return []

    def get_relationships(self) -> list[dict[str, Any]]:
        """
        Get all relationships from the model using DMV queries.

        Returns:
            List of relationship definitions
        """
        try:
            dmv_query = "SELECT [Name], [FromTableName], [FromColumnName], [ToTableName], [ToColumnName], [IsActive] FROM $SYSTEM.TMSCHEMA_RELATIONSHIPS"
            rel_rows = self._execute_dmv_query(dmv_query)
            
            relationships = []
            for rel_row in rel_rows:
                rel_name = rel_row.get("[Name]", "")
                if rel_name:
                    relationships.append({
                        "name": rel_name,
                        "fromTable": rel_row.get("[FromTableName]", ""),
                        "fromColumn": rel_row.get("[FromColumnName]", ""),
                        "toTable": rel_row.get("[ToTableName]", ""),
                        "toColumn": rel_row.get("[ToColumnName]", ""),
                        "isActive": rel_row.get("[IsActive]", True),
                        "crossFilteringBehavior": "OneDirection"  # Default, not always in DMV
                    })
            
            logger.info(f"Retrieved {len(relationships)} relationships via DMV")
            return relationships
            
        except Exception as e:
            logger.error(f"Failed to retrieve relationships via DMV: {e}")
            return []

    def disconnect(self) -> None:
        """Disconnect from XMLA endpoint."""
        self._token = None
        logger.info("Disconnected from XMLA endpoint")
