"""
Fabric semantic model parser.

Parses Power BI / Fabric semantic model definitions into normalized format.
"""

from __future__ import annotations


from typing import Any
import json
import base64

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
    DataType,
)
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.core.fabric_xmla_client import FabricXmlaClient
from semantic_sync.config.settings import FabricConfig
from semantic_sync.utils.exceptions import ResourceNotFoundError
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)

# Try to import OneLake client (optional dependency)
try:
    from semantic_sync.core.onelake_client import OneLakeClient
    ONELAKE_AVAILABLE = True
except ImportError:
    ONELAKE_AVAILABLE = False

# Try to import auto-metadata extractor
try:
    from semantic_sync.core.auto_metadata import get_auto_metadata_extractor
    AUTO_METADATA_AVAILABLE = True
except ImportError:
    AUTO_METADATA_AVAILABLE = False

# Try to import metadata registry (manual fallback)
try:
    from semantic_sync.core.metadata_registry import get_metadata_registry
    METADATA_REGISTRY_AVAILABLE = True
except ImportError:
    METADATA_REGISTRY_AVAILABLE = False


class FabricModelParser:
    """Parses Fabric datasets to normalized SemanticModel format."""

    def __init__(self, client: FabricClient, config: FabricConfig) -> None:
        self._client = client
        self._config = config
        self._xmla_client = None

    def read_semantic_model(self, dataset_id: str | None = None) -> SemanticModel:
        """Read and parse a semantic model from Fabric."""
        dataset_id = dataset_id or self._config.dataset_id
        logger.info(f"Reading Fabric semantic model: {dataset_id}")

        try:
            dataset_info = self._client.get_dataset(dataset_id)
        except ResourceNotFoundError:
            raise ResourceNotFoundError(
                f"Fabric dataset not found: {dataset_id}",
                resource_type="dataset",
                resource_id=dataset_id,
            )

        dataset_name = dataset_info.get("name", dataset_id)
        dataset_type = "push" if dataset_info.get("addRowsAPIEnabled", False) else "import"
        description = dataset_info.get("description", "")
        
        logger.info(f"Dataset '{dataset_name}' ({dataset_type}) - using REST API")
        
        # 0. QUICK CHECK: Use manual metadata if available (skips slow API calls)
        if METADATA_REGISTRY_AVAILABLE:
            try:
                registry = get_metadata_registry()
                logger.info(f"Checking manual definition for: '{dataset_name}'")
                has_def = registry.has_manual_definition(dataset_name)
                logger.info(f"Registry has definition? {has_def}")
                
                if has_def:
                    logger.info(f"Found manual definition for '{dataset_name}' - skipping API calls")
                    tables = registry.get_manual_tables(dataset_name)
                    manual_desc = registry.get_manual_description(dataset_name)
                    
                    return SemanticModel(
                        name=dataset_name,
                        id=dataset_id,
                        source="fabric",
                        description=manual_desc or description,
                        tables=tables,
                        measures=[],
                        relationships=[]
                    )
            except Exception as e:
                logger.warning(f"Quick metadata check failed: {e}")
            
        # 1. Get Tables (via standard REST API)
        tables = []
        try:
            tables_data = self._client.get_dataset_tables(dataset_id)
            tables = self._parse_tables(tables_data)
        except Exception as e:
            logger.warning(f"Failed to read tables via standard REST API: {e}")

        # 2. Get Measures, Relationships, and Table fallback (via DMV)
        measures = []
        relationships = []
        
        try:
            # Initialize XMLA/DMV client (uses REST executeQueries)
            xmla_client = FabricXmlaClient(config=self._config)
            # Ensure we target the correct dataset
            xmla_client._dataset_id = dataset_id 
            xmla_client.connect(dataset_name, dataset_name)
            
            # Fetch Measures
            measures_data = xmla_client.get_measures()
            measures = self._parse_measures(measures_data)
            
            # Fetch Relationships
            relationships_data = xmla_client.get_relationships()
            relationships = self._parse_relationships(relationships_data)
            
            # Fallback/Enrichment for tables
            # If REST returned no tables OR tables with no columns (common in Import mode)
            tables_with_columns = sum(1 for t in tables if t.columns)
            if not tables or tables_with_columns == 0:
                logger.info("Attempting to fetch table metadata via DMV...")
                tables_dmv = xmla_client.get_tables()
                if tables_dmv:
                    tables = self._parse_tables(tables_dmv)
                    
        except Exception as e:
            logger.warning(f"Metadata enrichment via DMV failed: {e}")
            
        # 3. New Fallback: Fabric Definition API (BIM)
        # Bypasses 404/400 errors for empty/unprocessed models by fetching the definition directly
        if not tables or sum(1 for t in tables if t.columns) == 0:
            logger.info(f"Attempting to fetch model definition via Fabric API...")
            try:
                definition = self._client.get_semantic_model_definition(dataset_id)
                if definition:
                    bim_tables = self._parse_bim_definition(definition)
                    if bim_tables:
                        tables = bim_tables
                        logger.info(f"Loaded {len(tables)} tables from Fabric definition API")
            except Exception as e:
                logger.warning(f"Definition API fallback failed: {e}")
        
        # 3. Final fallback: Try OneLake for Lakehouse-backed datasets
        if not tables and ONELAKE_AVAILABLE:
            target_storage = dataset_info.get("targetStorageMode", "")
            if target_storage in ("PremiumFiles", "Lakehouse"):
                logger.info(f"Attempting OneLake fallback for Lakehouse-backed dataset")
                try:
                    tables = self._get_tables_via_onelake(dataset_name)
                except Exception as e:
                    logger.warning(f"OneLake fallback failed: {e}")
        
        # 4. Auto-metadata fallback: Use pre-defined metadata definitions
        if not tables and AUTO_METADATA_AVAILABLE:
            logger.info(f"Attempting auto-metadata fallback for '{dataset_name}'")
            try:
                extractor = get_auto_metadata_extractor()
                if extractor.has_manual_definition(dataset_name):
                    tables = extractor.get_manual_tables(dataset_name)
                    logger.info(f"Loaded {len(tables)} tables from auto-metadata for '{dataset_name}'")
            except Exception as e:
                logger.warning(f"Auto-metadata fallback failed: {e}")
        
        # 5. Metadata registry fallback: Use manually defined metadata
        if not tables and METADATA_REGISTRY_AVAILABLE:
            logger.info(f"Attempting metadata registry fallback for '{dataset_name}'")
            try:
                registry = get_metadata_registry()
                if registry.has_manual_definition(dataset_name):
                    tables = registry.get_manual_tables(dataset_name)
                    # Also get manual description if available
                    manual_desc = registry.get_manual_description(dataset_name)
                    if manual_desc and not description:
                        description = manual_desc
                    logger.info(f"✅ Loaded {len(tables)} tables from metadata registry for '{dataset_name}'")
            except Exception as e:
                logger.warning(f"Metadata registry fallback failed: {e}")

        if not tables and not measures:
             logger.warning("No tables or measures found. Model might be empty.")

        model = SemanticModel(
            name=dataset_name,
            source="fabric",
            description=description,
            tables=tables,
            measures=measures,
            relationships=relationships,
            metadata={
                "dataset_id": dataset_id,
                "workspace_id": self._config.workspace_id,
                "dataset_type": dataset_type,
            },
        )

        logger.info(f"Parsed Fabric model: {len(tables)} tables, {len(measures)} measures, {len(relationships)} relationships")
        return model

    def _read_via_xmla(self, dataset_name: str) -> tuple[list[SemanticTable], list[SemanticMeasure], list[SemanticRelationship]]:
        """
        Read model schema via XMLA endpoint for Import datasets.
        
        Args:
            dataset_name: Name of the dataset
            
        Returns:
            Tuple of (tables, measures, relationships)
        """
        try:
            # Get workspace name - we need to query for it
            workspace_id = self._config.workspace_id
            workspace_info = self._client.get_workspace(workspace_id)
            workspace_name = workspace_info.get("name", workspace_id)
            
            # Create XMLA client
            self._xmla_client = FabricXmlaClient(config=self._config)
            
            # Connect to dataset
            self._xmla_client.connect(workspace_name, dataset_name)
            
            # Get tables
            tables_data = self._xmla_client.get_tables()
            tables = self._parse_tables(tables_data)
            
            # Get measures
            measures_data = self._xmla_client.get_measures()
            measures = self._parse_measures(measures_data)
            
            # Get relationships
            relationships_data = self._xmla_client.get_relationships()
            relationships = self._parse_relationships(relationships_data)
            
            # Disconnect
            self._xmla_client.disconnect()
            
            return tables, measures, relationships
            
        except Exception as e:
            logger.error(f"Failed to read via XMLA: {e}")
            if self._xmla_client:
                self._xmla_client.disconnect()
            raise ResourceNotFoundError(
                f"Failed to read model via XMLA: {e}",
                resource_type="dataset",
                resource_id=dataset_name
            )
    
    def _get_tables_via_onelake(self, dataset_name: str) -> list[SemanticTable]:
        """
        Get tables from OneLake for Lakehouse-backed datasets.
        
        Args:
            dataset_name: Name of the dataset/Lakehouse
            
        Returns:
            List of SemanticTable objects
        """
        if not ONELAKE_AVAILABLE:
            return []
        
        try:
            # Get workspace name
            workspace_info = self._client.get_workspace(self._config.workspace_id)
            workspace_name = workspace_info.get("name", "")
            
            if not workspace_name:
                logger.warning("Could not determine workspace name for OneLake")
                return []
            
            # Create OneLake client
            onelake_client = OneLakeClient(self._config)
            
            # Get tables from Lakehouse
            tables_data = onelake_client.get_lakehouse_tables(workspace_name, dataset_name)
            
            if tables_data:
                logger.info(f"Retrieved {len(tables_data)} tables via OneLake")
                return self._parse_tables(tables_data)
            
            return []
            
        except Exception as e:
            logger.warning(f"OneLake table extraction failed: {e}")
            return []

    def _parse_tables(self, tables_data: list[dict[str, Any]]) -> list[SemanticTable]:
        """Parse table definitions from Fabric API response."""
        tables: list[SemanticTable] = []

        for table_data in tables_data:
            table_name = table_data.get("name", "")
            if not table_name:
                continue

            columns = self._parse_columns(table_data.get("columns", []))
            tables.append(
                SemanticTable(
                    name=table_name,
                    description=table_data.get("description", ""),
                    columns=columns,
                    is_hidden=table_data.get("isHidden", False),
                )
            )

        return tables

    def _parse_columns(self, columns_data: list[dict[str, Any]]) -> list[SemanticColumn]:
        """Parse column definitions."""
        columns: list[SemanticColumn] = []

        for col_data in columns_data:
            col_name = col_data.get("name", "")
            if not col_name:
                continue

            data_type = col_data.get("dataType", "String")
            columns.append(
                SemanticColumn(
                    name=col_name,
                    data_type=data_type,
                    normalized_type=DataType.from_fabric(data_type),
                    is_nullable=col_data.get("isNullable", True),
                    description=col_data.get("description", ""),
                    is_hidden=col_data.get("isHidden", False),
                )
            )

        return columns

    def _parse_measures(self, measures_data: list[dict[str, Any]]) -> list[SemanticMeasure]:
        """Parse measure definitions from XMLA."""
        measures: list[SemanticMeasure] = []

        for measure_data in measures_data:
            measure_name = measure_data.get("name", "")
            if not measure_name:
                continue

            measures.append(
                SemanticMeasure(
                    name=measure_name,
                    expression=measure_data.get("expression", ""),
                    description=measure_data.get("description", ""),
                    table_name=measure_data.get("table", ""),
                    is_hidden=measure_data.get("isHidden", False),
                )
            )

        return measures

    def _parse_relationships(self, relationships_data: list[dict[str, Any]]) -> list[SemanticRelationship]:
        """Parse relationship definitions from XMLA."""
        relationships: list[SemanticRelationship] = []

        for rel_data in relationships_data:
            rel_name = rel_data.get("name", "")
            if not rel_name:
                continue

            relationships.append(
                SemanticRelationship(
                    name=rel_name,
                    from_table=rel_data.get("fromTable", ""),
                    from_column=rel_data.get("fromColumn", ""),
                    to_table=rel_data.get("toTable", ""),
                    to_column=rel_data.get("toColumn", ""),
                    is_active=rel_data.get("isActive", True),
                    cross_filtering_behavior=rel_data.get("crossFilteringBehavior", "OneDirection"),
                )
            )

        return relationships

    def _parse_bim_definition(self, definition_response: dict[str, Any]) -> list[SemanticTable]:
        """Parse tables from BIM definition response."""
        tables: list[SemanticTable] = []
        
        try:
            # Handle response structure (sometimes nested in result)
            definition = definition_response.get("definition", {})
            if not definition and "result" in definition_response:
                definition = definition_response["result"].get("definition", {})
                
            parts = definition.get("parts", [])
            for part in parts:
                if part.get("path") == "model.bim":
                    payload = part.get("payload")
                    payload_type = part.get("payloadType")
                    
                    if payload_type == "InlineBase64" and payload:
                        try:
                            decoded = base64.b64decode(payload).decode("utf-8")
                            model_json = json.loads(decoded)
                            model_data = model_json.get("model", {})
                            
                            for t_data in model_data.get("tables", []):
                                t_name = t_data.get("name")
                                if not t_name: 
                                    continue
                                    
                                # Parse columns
                                columns = []
                                for c_data in t_data.get("columns", []):
                                    c_name = c_data.get("name")
                                    if not c_name:
                                        continue
                                    
                                    dtype = c_data.get("dataType", "string")  # Default to string
                                    # Normalize type if needed or use raw
                                    
                                    columns.append(SemanticColumn(
                                        name=c_name,
                                        data_type=dtype,
                                        normalized_type=DataType.from_fabric(dtype),
                                        is_nullable=c_data.get("isNullable", True),
                                        description=c_data.get("description", ""),
                                        is_hidden=c_data.get("isHidden", False)
                                    ))
                                
                                tables.append(SemanticTable(
                                    name=t_name,
                                    description=t_data.get("description", ""),
                                    columns=columns,
                                    is_hidden=t_data.get("isHidden", False)
                                ))
                                
                        except Exception as e:
                            logger.error(f"Failed to parse model.bim payload: {e}")
                            
        except Exception as e:
             logger.error(f"Error processing definition response: {e}")
             
        return tables

