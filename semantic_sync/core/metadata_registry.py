"""
Metadata registry for manual model definitions.

This module provides a fallback mechanism for semantic models that cannot be
read via standard APIs (REST API, DMV, XMLA). It allows manual definition
of table and column metadata for models.
"""

from __future__ import annotations

from typing import Any
import os
import yaml
import json
from pathlib import Path

from semantic_sync.core.models import SemanticTable, SemanticColumn, DataType
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


# Built-in metadata registry for known models
MANUAL_METADATA_REGISTRY: dict[str, dict[str, Any]] = {
    "new_rep": {
        "description": "Sales Representatives dataset",
        "tables": [
            {
                "name": "Representatives",
                "description": "Sales representatives information",
                "columns": [
                    {"name": "rep_id", "dataType": "Int64", "description": "Representative ID"},
                    {"name": "name", "dataType": "String", "description": "Representative name"},
                    {"name": "region", "dataType": "String", "description": "Assigned region"},
                    {"name": "email", "dataType": "String", "description": "Contact email"},
                ]
            }
        ]
    }
}


class MetadataRegistry:
    """Manages manual metadata definitions for semantic models."""
    
    def __init__(self, registry_dir: str | None = None):
        """
        Initialize the metadata registry.
        
        Args:
            registry_dir: Optional directory containing YAML/JSON metadata files
        """
        self.registry_dir = Path(registry_dir) if registry_dir else None
        self._file_metadata_cache: dict[str, dict[str, Any]] = {}
        
        # Load metadata from files if directory exists
        if self.registry_dir and self.registry_dir.exists():
            self._load_metadata_files()
    
    def _load_metadata_files(self) -> None:
        """Load all metadata definition files from the registry directory."""
        if not self.registry_dir:
            return
        
        logger.info(f"Loading metadata files from {self.registry_dir}")
        
        # Support both YAML and JSON files
        for pattern in ["*.yaml", "*.yml", "*.json"]:
            for file_path in self.registry_dir.glob(pattern):
                try:
                    model_name = file_path.stem
                    
                    with open(file_path, 'r') as f:
                        if file_path.suffix in ['.yaml', '.yml']:
                            metadata = yaml.safe_load(f)
                        else:
                            metadata = json.load(f)
                    
                    self._file_metadata_cache[model_name] = metadata
                    logger.info(f"Loaded metadata for model '{model_name}' from {file_path.name}")
                    
                except Exception as e:
                    logger.warning(f"Failed to load metadata file {file_path}: {e}")
    
    def has_manual_definition(self, model_name: str) -> bool:
        """
        Check if a manual metadata definition exists for the model.
        
        Args:
            model_name: Name of the model to check
            
        Returns:
            True if manual definition exists
        """
        return (
            model_name.lower() in MANUAL_METADATA_REGISTRY or
            model_name in self._file_metadata_cache
        )
    
    def get_manual_tables(self, model_name: str) -> list[SemanticTable]:
        """
        Get manually defined tables for a model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            List of SemanticTable objects
        """
        # Check file-based metadata first (takes precedence)
        if model_name in self._file_metadata_cache:
            metadata = self._file_metadata_cache[model_name]
            logger.info(f"Using file-based metadata for '{model_name}'")
        # Fall back to built-in registry
        elif model_name.lower() in MANUAL_METADATA_REGISTRY:
            metadata = MANUAL_METADATA_REGISTRY[model_name.lower()]
            logger.info(f"Using built-in metadata registry for '{model_name}'")
        else:
            logger.warning(f"No manual metadata found for '{model_name}'")
            return []
        
        # Parse tables
        tables_data = metadata.get("tables", [])
        return self._parse_tables(tables_data)
    
    def get_manual_description(self, model_name: str) -> str:
        """
        Get manual description for a model.
        
        Args:
            model_name: Name of the model
            
        Returns:
            Model description or empty string
        """
        if model_name in self._file_metadata_cache:
            return self._file_metadata_cache[model_name].get("description", "")
        elif model_name.lower() in MANUAL_METADATA_REGISTRY:
            return MANUAL_METADATA_REGISTRY[model_name.lower()].get("description", "")
        return ""
    
    def _parse_tables(self, tables_data: list[dict[str, Any]]) -> list[SemanticTable]:
        """Parse table definitions from manual metadata."""
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
        """Parse column definitions from manual metadata."""
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
    
    def add_model_metadata(
        self, 
        model_name: str, 
        tables: list[dict[str, Any]],
        description: str = ""
    ) -> None:
        """
        Add or update manual metadata for a model.
        
        Args:
            model_name: Name of the model
            tables: List of table definitions
            description: Model description
        """
        metadata = {
            "description": description,
            "tables": tables
        }
        
        # If registry directory exists, save to file
        if self.registry_dir and self.registry_dir.exists():
            file_path = self.registry_dir / f"{model_name}.yaml"
            try:
                with open(file_path, 'w') as f:
                    yaml.dump(metadata, f, default_flow_style=False)
                logger.info(f"Saved metadata for '{model_name}' to {file_path}")
                self._file_metadata_cache[model_name] = metadata
            except Exception as e:
                logger.error(f"Failed to save metadata file: {e}")
        else:
            # Add to in-memory registry
            MANUAL_METADATA_REGISTRY[model_name.lower()] = metadata
            logger.info(f"Added metadata for '{model_name}' to in-memory registry")


def get_metadata_registry(registry_dir: str | None = None) -> MetadataRegistry:
    """
    Get or create a metadata registry instance.
    
    Args:
        registry_dir: Optional directory containing metadata files
        
    Returns:
        MetadataRegistry instance
    """
    # Default to metadata directory in project root
    if registry_dir is None:
        # Try to find project root by looking for pyproject.toml
        current = Path(__file__).parent
        while current != current.parent:
            if (current / "pyproject.toml").exists():
                registry_dir = str(current / "metadata")
                break
            current = current.parent
    
    return MetadataRegistry(registry_dir)
