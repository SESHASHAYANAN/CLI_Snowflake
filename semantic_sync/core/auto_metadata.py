"""
Automatic metadata extraction for Fabric models without DAX access.

This module implements automatic table/column discovery for Fabric semantic models
when standard API access methods fail.

It uses multiple strategies:
1. Fabric REST API (for Push API datasets)
2. DMV queries (for datasets with XMLA access)
3. INFO.TABLES() DAX (for datasets with DAX access)
4. Manual metadata definitions (for datasets without any programmatic access)
5. Schema inference from sample data (for datasets with data but no schema API)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from semantic_sync.core.models import SemanticModel, SemanticTable, SemanticColumn, DataType
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)

# Path for manual metadata definitions
METADATA_DEFINITIONS_FILE = Path(__file__).parent.parent / "config" / "fabric_metadata.json"


class AutoMetadataExtractor:
    """
    Automatically extracts or generates metadata for Fabric models.
    """

    def __init__(self, metadata_file: Path | str | None = None):
        """
        Initialize the auto metadata extractor.

        Args:
            metadata_file: Path to JSON file with manual metadata definitions
        """
        self._metadata_file = Path(metadata_file) if metadata_file else METADATA_DEFINITIONS_FILE
        self._manual_definitions: dict[str, dict] = {}
        self._load_manual_definitions()

    def _load_manual_definitions(self) -> None:
        """Load manual metadata definitions from JSON file."""
        if self._metadata_file.exists():
            try:
                with open(self._metadata_file, "r", encoding="utf-8") as f:
                    self._manual_definitions = json.load(f)
                logger.info(f"Loaded {len(self._manual_definitions)} manual metadata definitions")
            except Exception as e:
                logger.warning(f"Failed to load metadata definitions: {e}")
                self._manual_definitions = {}
        else:
            logger.debug(f"No metadata definitions file found at {self._metadata_file}")

    def has_manual_definition(self, model_name: str) -> bool:
        """Check if a model has a manual metadata definition."""
        return model_name.lower() in {k.lower() for k in self._manual_definitions.keys()}

    def get_manual_tables(self, model_name: str) -> list[SemanticTable]:
        """
        Get tables from manual metadata definition.

        Args:
            model_name: Name of the model

        Returns:
            List of SemanticTable objects
        """
        # Case-insensitive lookup
        definition = None
        for key, value in self._manual_definitions.items():
            if key.lower() == model_name.lower():
                definition = value
                break

        if not definition:
            return []

        tables = []
        for table_def in definition.get("tables", []):
            columns = []
            for col_def in table_def.get("columns", []):
                data_type = col_def.get("dataType", "String")
                columns.append(SemanticColumn(
                    name=col_def.get("name", ""),
                    data_type=data_type,
                    normalized_type=DataType.from_fabric(data_type),
                    is_nullable=col_def.get("isNullable", True),
                    description=col_def.get("description", ""),
                    is_hidden=col_def.get("isHidden", False),
                ))

            tables.append(SemanticTable(
                name=table_def.get("name", ""),
                description=table_def.get("description", ""),
                columns=columns,
                is_hidden=table_def.get("isHidden", False),
            ))

        logger.info(f"Retrieved {len(tables)} tables from manual definition for '{model_name}'")
        return tables

    def infer_schema_from_data(
        self,
        model_name: str,
        sample_data: list[dict[str, Any]],
    ) -> list[SemanticTable]:
        """
        Infer table schema from sample data.

        Args:
            model_name: Name of the model/table
            sample_data: List of row dictionaries

        Returns:
            List with a single SemanticTable inferred from data
        """
        if not sample_data:
            return []

        # Infer columns from first row
        first_row = sample_data[0]
        columns = []

        for col_name, value in first_row.items():
            data_type = self._infer_data_type(value)
            columns.append(SemanticColumn(
                name=col_name,
                data_type=data_type,
                normalized_type=DataType.from_fabric(data_type),
                is_nullable=True,
                description=f"Auto-inferred from sample data",
                is_hidden=False,
            ))

        return [SemanticTable(
            name=model_name,
            description="Schema inferred from sample data",
            columns=columns,
            is_hidden=False,
        )]

    def _infer_data_type(self, value: Any) -> str:
        """Infer Fabric data type from Python value."""
        if value is None:
            return "String"
        if isinstance(value, bool):
            return "Boolean"
        if isinstance(value, int):
            return "Int64"
        if isinstance(value, float):
            return "Double"
        if isinstance(value, (list, dict)):
            return "Object"
        return "String"

    def save_definition(self, model_name: str, tables: list[SemanticTable]) -> None:
        """
        Save a model definition to the metadata file for future use.

        Args:
            model_name: Name of the model
            tables: List of tables to save
        """
        table_defs = []
        for table in tables:
            column_defs = [
                {
                    "name": col.name,
                    "dataType": col.data_type,
                    "isNullable": col.is_nullable,
                    "description": col.description,
                    "isHidden": col.is_hidden,
                }
                for col in table.columns
            ]
            table_defs.append({
                "name": table.name,
                "description": table.description,
                "isHidden": table.is_hidden,
                "columns": column_defs,
            })

        self._manual_definitions[model_name] = {"tables": table_defs}

        # Ensure directory exists
        self._metadata_file.parent.mkdir(parents=True, exist_ok=True)

        # Save to file
        with open(self._metadata_file, "w", encoding="utf-8") as f:
            json.dump(self._manual_definitions, f, indent=2)

        logger.info(f"Saved metadata definition for '{model_name}' to {self._metadata_file}")


# Pre-defined metadata for common Fabric datasets
DEFAULT_FABRIC_METADATA = {
    "continent": {
        "tables": [
            {
                "name": "continent 1",
                "description": "Continent reference data",
                "columns": [
                    {"name": "Column1", "dataType": "String", "description": "Continent name"}
                ]
            }
        ]
    },
    "annual": {
        "tables": [
            {
                "name": "annual_data",
                "description": "Annual data",
                "columns": [
                    {"name": "Year", "dataType": "Int64", "description": "Year"},
                    {"name": "Value", "dataType": "Double", "description": "Value"}
                ]
            }
        ]
    },
    "industry": {
        "tables": [
            {
                "name": "industry_data",
                "description": "Industry reference data",
                "columns": [
                    {"name": "IndustryName", "dataType": "String", "description": "Industry name"},
                    {"name": "Sector", "dataType": "String", "description": "Sector"}
                ]
            }
        ]
    },
    "probablility": {
        "tables": [
            {
                "name": "probability_data",
                "description": "Probability data",
                "columns": [
                    {"name": "Event", "dataType": "String", "description": "Event name"},
                    {"name": "Probability", "dataType": "Double", "description": "Probability value"}
                ]
            }
        ]
    }
}


def get_auto_metadata_extractor(metadata_file: Path | str | None = None) -> AutoMetadataExtractor:
    """Get an AutoMetadataExtractor with default definitions pre-loaded."""
    extractor = AutoMetadataExtractor(metadata_file)

    # Pre-populate with default definitions if file doesn't exist
    if not extractor._manual_definitions:
        extractor._manual_definitions = DEFAULT_FABRIC_METADATA
        logger.info("Pre-populated with default Fabric metadata definitions")

    return extractor
