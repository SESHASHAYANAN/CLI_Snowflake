"""
Fabric semantic model parser.

Parses Power BI / Fabric semantic model definitions into normalized format.
"""

from typing import Any

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


class FabricModelParser:
    """Parses Fabric datasets to normalized SemanticModel format."""

    def __init__(self, client: FabricClient, config: FabricConfig) -> None:
        self._client = client
        self._config = config
        self._xmla_client = None

    def read_semantic_model(self) -> SemanticModel:
        """Read and parse the configured semantic model from Fabric."""
        dataset_id = self._config.dataset_id
        logger.info(f"Reading Fabric semantic model: {dataset_id}")

        try:
            dataset_info = self._client.get_dataset(dataset_id)
        except ResourceNotFoundError:
            raise ResourceNotFoundError(
                f"Fabric dataset not found: {dataset_id}",
                resource_type="dataset",
                resource_id=dataset_id,
            )

        # Check if this is a Push API dataset or Import dataset
        is_push_dataset = dataset_info.get("addRowsAPIEnabled", False)
        dataset_name = dataset_info.get("name", dataset_id)
        
        if is_push_dataset:
            logger.info(f"Dataset '{dataset_name}' is a Push API dataset - using REST API")
            tables_data = self._client.get_dataset_tables(dataset_id)
            
            # Check if we got columns - if not, we might need to fallback to XMLA
            has_columns = any(t.get("columns") for t in tables_data) if tables_data else False
            
            tables = []
            measures = []
            relationships = []
            
            if tables_data and not has_columns:
                logger.warning("REST API returned tables without columns - attempting XMLA/DMV")
                xmla_tables, xmla_measures, xmla_rels = self._read_via_xmla(dataset_name)
                
                if not xmla_tables and tables_data:
                    logger.warning("XMLA/DMV returned no tables (likely failed) - falling back to REST tables (metadata only)")
                    # Fallback to REST tables (even if columns are missing)
                    # This allows 'Add Column' logic to proceed for existing tables
                    tables = self._parse_tables(tables_data)
                else:
                    tables = xmla_tables
                    measures = xmla_measures
                    relationships = xmla_rels
            else:
                tables = self._parse_tables(tables_data)
        else:
            logger.info(f"Dataset '{dataset_name}' is an Import dataset - using XMLA endpoint")
            tables, measures, relationships = self._read_via_xmla(dataset_name)

        model = SemanticModel(
            name=dataset_name,
            source="fabric",
            description=dataset_info.get("description", ""),
            tables=tables,
            measures=measures,
            relationships=relationships,
            metadata={
                "dataset_id": dataset_id,
                "workspace_id": self._config.workspace_id,
                "dataset_type": "push" if is_push_dataset else "import",
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
                    table=measure_data.get("table", ""),
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
