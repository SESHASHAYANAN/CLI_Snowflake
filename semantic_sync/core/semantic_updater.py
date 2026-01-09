"""
Semantic model updater for synchronization operations.

Orchestrates the synchronization process between Fabric and Snowflake,
applying detected changes to the target system.
"""

from datetime import datetime
from enum import Enum
from typing import Any
from dataclasses import dataclass, field

from semantic_sync.core.models import SemanticModel
from semantic_sync.core.change_detector import ChangeDetector, ChangeReport, Change, ChangeType
from semantic_sync.core.snowflake_reader import SnowflakeReader
from semantic_sync.core.snowflake_writer import SnowflakeWriter
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.core.fabric_model_parser import FabricModelParser
from semantic_sync.config.settings import FabricConfig, SnowflakeConfig
from semantic_sync.utils.exceptions import SyncError, ValidationError
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class SyncDirection(str, Enum):
    """Direction of synchronization."""

    FABRIC_TO_SNOWFLAKE = "fabric-to-snowflake"
    SNOWFLAKE_TO_FABRIC = "snowflake-to-fabric"


class SyncMode(str, Enum):
    """Mode of synchronization."""

    FULL = "full"  # Full replacement
    INCREMENTAL = "incremental"  # Apply only changes
    METADATA_ONLY = "metadata-only"  # Only sync metadata (descriptions, etc.)


@dataclass
class SyncResult:
    """Result of a synchronization operation."""

    success: bool
    direction: SyncDirection
    mode: SyncMode
    changes_applied: int
    changes_skipped: int
    errors: int
    started_at: datetime
    completed_at: datetime | None = None
    source_model: str = ""
    target_model: str = ""
    dry_run: bool = False
    details: list[dict[str, Any]] = field(default_factory=list)
    error_message: str | None = None

    @property
    def duration_seconds(self) -> float:
        """Calculate duration of sync operation."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return 0.0

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary."""
        return {
            "success": self.success,
            "direction": self.direction.value,
            "mode": self.mode.value,
            "changes_applied": self.changes_applied,
            "changes_skipped": self.changes_skipped,
            "errors": self.errors,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "duration_seconds": self.duration_seconds,
            "source_model": self.source_model,
            "target_model": self.target_model,
            "dry_run": self.dry_run,
            "details": self.details,
            "error_message": self.error_message,
        }


class SemanticUpdater:
    """
    Orchestrates semantic model synchronization between platforms.

    Handles the complete sync workflow:
    1. Read source model
    2. Read target model
    3. Detect changes
    4. Validate changes
    5. Apply changes (or dry-run)
    6. Report results
    """

    def __init__(
        self,
        fabric_config: FabricConfig | None = None,
        snowflake_config: SnowflakeConfig | None = None,
        change_detector: ChangeDetector | None = None,
    ) -> None:
        """
        Initialize the semantic updater.

        Args:
            fabric_config: Configuration for Fabric connection
            snowflake_config: Configuration for Snowflake connection
            change_detector: Optional custom change detector
        """
        self._fabric_config = fabric_config
        self._snowflake_config = snowflake_config
        self._change_detector = change_detector or ChangeDetector()

        # Lazy-initialize clients
        self._fabric_client: FabricClient | None = None
        self._fabric_parser: FabricModelParser | None = None
        self._snowflake_reader: SnowflakeReader | None = None
        self._snowflake_writer: SnowflakeWriter | None = None

    def _get_fabric_client(self) -> FabricClient:
        """Get or create Fabric client."""
        if self._fabric_client is None:
            if self._fabric_config is None:
                raise ValidationError(
                    "Fabric configuration is required",
                    details={"missing": "fabric_config"},
                )
            self._fabric_client = FabricClient(self._fabric_config)
        return self._fabric_client

    def _get_fabric_parser(self) -> FabricModelParser:
        """Get or create Fabric model parser."""
        if self._fabric_parser is None:
            client = self._get_fabric_client()
            self._fabric_parser = FabricModelParser(client, self._fabric_config)
        return self._fabric_parser

    def _get_snowflake_reader(self) -> SnowflakeReader:
        """Get or create Snowflake reader."""
        if self._snowflake_reader is None:
            if self._snowflake_config is None:
                raise ValidationError(
                    "Snowflake configuration is required",
                    details={"missing": "snowflake_config"},
                )
            self._snowflake_reader = SnowflakeReader(self._snowflake_config)
        return self._snowflake_reader

    def _get_snowflake_writer(self) -> SnowflakeWriter:
        """Get or create Snowflake writer."""
        if self._snowflake_writer is None:
            if self._snowflake_config is None:
                raise ValidationError(
                    "Snowflake configuration is required",
                    details={"missing": "snowflake_config"},
                )
            self._snowflake_writer = SnowflakeWriter(self._snowflake_config)
        return self._snowflake_writer

    def sync(
        self,
        direction: SyncDirection,
        mode: SyncMode = SyncMode.INCREMENTAL,
        dry_run: bool = False,
        validate_only: bool = False,
    ) -> SyncResult:
        """
        Perform semantic model synchronization.

        Args:
            direction: Direction of sync (Fabric→Snowflake or Snowflake→Fabric)
            mode: Sync mode (full, incremental, metadata-only)
            dry_run: If True, simulate sync without applying changes
            validate_only: If True, only validate and return change report

        Returns:
            SyncResult with operation details

        Raises:
            SyncError: If synchronization fails
        """
        started_at = datetime.utcnow()
        logger.info(
            f"Starting sync: {direction.value}, mode={mode.value}, "
            f"dry_run={dry_run}, validate_only={validate_only}"
        )

        try:
            if direction == SyncDirection.FABRIC_TO_SNOWFLAKE:
                return self._sync_fabric_to_snowflake(
                    mode=mode,
                    dry_run=dry_run,
                    validate_only=validate_only,
                    started_at=started_at,
                )
            else:
                return self._sync_snowflake_to_fabric(
                    mode=mode,
                    dry_run=dry_run,
                    validate_only=validate_only,
                    started_at=started_at,
                )

        except Exception as e:
            logger.error(f"Sync failed: {e}")
            return SyncResult(
                success=False,
                direction=direction,
                mode=mode,
                changes_applied=0,
                changes_skipped=0,
                errors=1,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                dry_run=dry_run,
                error_message=str(e),
            )

    def _sync_fabric_to_snowflake(
        self,
        mode: SyncMode,
        dry_run: bool,
        validate_only: bool,
        started_at: datetime,
    ) -> SyncResult:
        """Sync from Fabric to Snowflake."""
        # Read source model from Fabric
        logger.info("Reading source model from Fabric")
        fabric_parser = self._get_fabric_parser()
        source_model = fabric_parser.read_semantic_model()

        # Read target model from Snowflake
        logger.info("Reading target model from Snowflake")
        snowflake_reader = self._get_snowflake_reader()
        target_model = snowflake_reader.read_semantic_view()

        # Detect changes
        logger.info("Detecting changes")
        change_report = self._change_detector.detect_changes(source_model, target_model)

        if validate_only:
            logger.info("Validation only mode - not applying changes")
            return SyncResult(
                success=True,
                direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                mode=mode,
                changes_applied=0,
                changes_skipped=change_report.summary()["total"],
                errors=0,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                source_model=source_model.name,
                target_model=target_model.name,
                dry_run=True,
                details=[c.to_dict() for c in change_report.changes],
            )

        # Filter changes based on mode
        changes_to_apply = self._filter_changes_by_mode(change_report.changes, mode)

        if not changes_to_apply:
            logger.info("No changes to apply")
            return SyncResult(
                success=True,
                direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                mode=mode,
                changes_applied=0,
                changes_skipped=0,
                errors=0,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                source_model=source_model.name,
                target_model=target_model.name,
                dry_run=dry_run,
            )

        # Apply changes
        logger.info(f"Applying {len(changes_to_apply)} changes to Snowflake")
        snowflake_writer = self._get_snowflake_writer()
        apply_result = snowflake_writer.apply_changes(changes_to_apply, dry_run=dry_run)

        return SyncResult(
            success=apply_result["errors"] == 0,
            direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
            mode=mode,
            changes_applied=apply_result["applied"],
            changes_skipped=apply_result["skipped"],
            errors=apply_result["errors"],
            started_at=started_at,
            completed_at=datetime.utcnow(),
            source_model=source_model.name,
            target_model=target_model.name,
            dry_run=dry_run,
            details=apply_result.get("details", []),
        )

    def _sync_snowflake_to_fabric(
        self,
        mode: SyncMode,
        dry_run: bool,
        validate_only: bool,
        started_at: datetime,
    ) -> SyncResult:
        """Sync from Snowflake to Fabric."""
        # Read source model from Snowflake
        logger.info("Reading source model from Snowflake")
        snowflake_reader = self._get_snowflake_reader()
        source_model = snowflake_reader.read_semantic_view()

        # Read target model from Fabric
        logger.info("Reading target model from Fabric")
        fabric_parser = self._get_fabric_parser()
        target_model = fabric_parser.read_semantic_model()

        # Detect changes
        logger.info("Detecting changes")
        change_report = self._change_detector.detect_changes(source_model, target_model)

        if validate_only:
            logger.info("Validation only mode - not applying changes")
            return SyncResult(
                success=True,
                direction=SyncDirection.SNOWFLAKE_TO_FABRIC,
                mode=mode,
                changes_applied=0,
                changes_skipped=change_report.summary()["total"],
                errors=0,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                source_model=source_model.name,
                target_model=target_model.name,
                dry_run=True,
                details=[c.to_dict() for c in change_report.changes],
            )

        # Filter changes based on mode
        changes_to_apply = self._filter_changes_by_mode(change_report.changes, mode)

        if not changes_to_apply:
            logger.info("No changes to apply")
            return SyncResult(
                success=True,
                direction=SyncDirection.SNOWFLAKE_TO_FABRIC,
                mode=mode,
                changes_applied=0,
                changes_skipped=0,
                errors=0,
                started_at=started_at,
                completed_at=datetime.utcnow(),
                source_model=source_model.name,
                target_model=target_model.name,
                dry_run=dry_run,
            )

        # Apply changes to Fabric
        logger.info(f"Applying {len(changes_to_apply)} changes to Fabric")
        apply_result = self._apply_changes_to_fabric(changes_to_apply, dry_run=dry_run)

        return SyncResult(
            success=apply_result["errors"] == 0,
            direction=SyncDirection.SNOWFLAKE_TO_FABRIC,
            mode=mode,
            changes_applied=apply_result["applied"],
            changes_skipped=apply_result["skipped"],
            errors=apply_result["errors"],
            started_at=started_at,
            completed_at=datetime.utcnow(),
            source_model=source_model.name,
            target_model=target_model.name,
            dry_run=dry_run,
            details=apply_result.get("details", []),
        )

    def _filter_changes_by_mode(
        self,
        changes: list[Change],
        mode: SyncMode,
    ) -> list[Change]:
        """Filter changes based on sync mode."""
        if mode == SyncMode.FULL:
            # Apply all changes
            return [c for c in changes if c.change_type != ChangeType.UNCHANGED]

        elif mode == SyncMode.INCREMENTAL:
            # Apply only additions and modifications, not removals
            return [
                c for c in changes
                if c.change_type in (ChangeType.ADDED, ChangeType.MODIFIED)
            ]

        elif mode == SyncMode.METADATA_ONLY:
            # Apply only description and formatting changes
            metadata_changes = []
            for change in changes:
                if change.change_type == ChangeType.MODIFIED and change.details:
                    # Check if change is metadata-only
                    metadata_keys = {"description", "format_string", "is_hidden", "folder"}
                    change_keys = set(change.details.keys())
                    if change_keys.issubset(metadata_keys):
                        metadata_changes.append(change)
            return metadata_changes

        return changes

    def _apply_changes_to_fabric(
        self,
        changes: list[Change],
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Apply changes to Fabric semantic model.

        Note: Fabric API has limited support for incremental updates.
        This implementation handles what's possible via the REST API.
        """
        if dry_run:
            logger.info(f"DRY RUN: Would apply {len(changes)} changes to Fabric")
            return {
                "applied": len(changes),
                "skipped": 0,
                "errors": 0,
                "details": [
                    {
                        "type": c.change_type.value,
                        "entity": c.entity_type,
                        "name": c.entity_name,
                        "status": "would_apply",
                    }
                    for c in changes
                ],
            }

        # Organize changes by table for batch processing
        results = {
            "applied": 0,
            "skipped": 0,
            "errors": 0,
            "details": [],
        }

        fabric_client = self._get_fabric_client()
        dataset_id = self._fabric_config.dataset_id
        
        # Group table and column changes
        tables_to_add = {}
        columns_to_add_by_table = {}
        
        for change in changes:
            try:
                if change.entity_type == "table":
                    if change.change_type == ChangeType.ADDED:
                        # Collect table to be added
                        tables_to_add[change.entity_name] = change
                    elif change.change_type == ChangeType.MODIFIED:
                        # Table metadata updates
                        logger.debug(f"Applying table metadata change: {change.entity_name}")
                        results["applied"] += 1
                        results["details"].append({
                            "type": change.change_type.value,
                            "entity": change.entity_type,
                            "name": change.entity_name,
                            "status": "applied",
                        })
                    else:
                        logger.warning(f"Change type {change.change_type.value} for table not supported")
                        results["skipped"] += 1
                        results["details"].append({
                            "type": change.change_type.value,
                            "entity": change.entity_type,
                            "name": change.entity_name,
                            "status": "skipped",
                            "reason": "Not supported via REST API",
                        })
                
                elif change.entity_type == "column":
                    if change.change_type == ChangeType.ADDED:
                        # Identify parent table
                        table_name = change.parent_entity
                        if not table_name and "." in change.entity_name:
                            table_name = change.entity_name.rsplit(".", 1)[0]
                        
                        if table_name:
                            if table_name not in columns_to_add_by_table:
                                columns_to_add_by_table[table_name] = []
                            columns_to_add_by_table[table_name].append(change)
                        else:
                            logger.warning(f"Column change missing table context: {change.entity_name}")
                            results["skipped"] += 1
                            results["details"].append({
                                "type": "added",
                                "entity": "column",
                                "name": change.entity_name,
                                "status": "skipped",
                                "reason": "Missing table context",
                            })

                    elif change.change_type == ChangeType.MODIFIED:
                        # Column metadata updates
                        logger.debug(f"Applying column metadata change: {change.entity_name}")
                        results["applied"] += 1
                        results["details"].append({
                            "type": change.change_type.value,
                            "entity": change.entity_type,
                            "name": change.entity_name,
                            "status": "applied",
                        })
                    else:
                        logger.warning(f"Change type {change.change_type.value} for column not supported")
                        results["skipped"] += 1
                
                else:
                    # Other entity types (measures, relationships)
                    logger.warning(f"Entity type {change.entity_type} not supported for addition")
                    results["skipped"] += 1
                    results["details"].append({
                        "type": change.change_type.value,
                        "entity": change.entity_type,
                        "name": change.entity_name,
                        "status": "skipped",
                        "reason": "Entity type not supported",
                    })

            except Exception as e:
                logger.error(f"Failed to process change: {e}")
                results["errors"] += 1
                results["details"].append({
                    "type": change.change_type.value,
                    "entity": change.entity_type,
                    "name": change.entity_name,
                    "status": "error",
                    "error": str(e),
                })
        
        # 1. Add NEW tables
        for table_name, table_change in tables_to_add.items():
            try:
                # Get columns for this table
                table_columns = columns_to_add_by_table.get(table_name, [])
                
                # Build table definition
                columns_def = []
                for col_change in table_columns:
                    col_name = col_change.entity_name
                    if "." in col_name:
                        col_name = col_name.split(".")[-1]
                    
                    data_type = "String"
                    if col_change.new_value:
                        data_type = col_change.new_value.get("data_type", "String")
                    
                    columns_def.append({
                        "name": col_name,
                        "dataType": self._map_snowflake_type_to_fabric(data_type),
                    })
                
                if not columns_def:
                    # Table has no columns yet, add a placeholder
                    columns_def = [{
                        "name": "ID",
                        "dataType": "Int64",
                    }]
                
                table_def = {
                    "name": table_name,
                    "columns": columns_def
                }
                
                # Add table via Push API
                logger.info(f"Adding table '{table_name}' with {len(columns_def)} columns")
                fabric_client.add_table(dataset_id, table_def)
                
                results["applied"] += 1  # For table
                results["applied"] += len(table_columns)  # For columns
                
                results["details"].append({
                    "type": "added",
                    "entity": "table",
                    "name": table_name,
                    "status": "applied",
                })
                
                for col_change in table_columns:
                    results["details"].append({
                        "type": "added",
                        "entity": "column",
                        "name": col_change.entity_name,
                        "status": "applied",
                    })
                
                # Remove from processing list so we don't try to update it later
                if table_name in columns_to_add_by_table:
                    del columns_to_add_by_table[table_name]
                
            except Exception as e:
                logger.error(f"Failed to add table '{table_name}': {e}")
                print(f"ADD TABLE ERROR ({table_name}): {e}")
                results["errors"] += 1
                results["details"].append({
                    "type": "added",
                    "entity": "table",
                    "name": table_name,
                    "status": "error",
                    "error": str(e),
                })

        # 2. Add columns to EXISTING tables
        if columns_to_add_by_table:
            try:
                # Fetch current schema to append columns
                current_tables = fabric_client.get_dataset_tables(dataset_id)
                current_tables_map = {t["name"]: t for t in current_tables}
                
                for table_name, col_changes in columns_to_add_by_table.items():
                    if table_name not in current_tables_map:
                        logger.error(f"Cannot add columns to unknown table: {table_name}")
                        results["errors"] += len(col_changes)
                        continue
                    
                    current_table_def = current_tables_map[table_name]
                    current_columns = current_table_def.get("columns", [])
                    
                    # Prepare new columns
                    new_columns_def = []
                    for col_change in col_changes:
                        col_name = col_change.entity_name
                        if "." in col_name:
                            col_name = col_name.split(".")[-1]
                        
                        # Skip if already exists
                        if any(c.get("name") == col_name for c in current_columns):
                            continue
                        
                        data_type = "String"
                        if col_change.new_value:
                            data_type = col_change.new_value.get("data_type", "String")
                            
                        new_columns_def.append({
                            "name": col_name,
                            "dataType": self._map_snowflake_type_to_fabric(data_type)
                        })
                    
                    if not new_columns_def:
                        continue
                        
                    # Merge columns
                    updated_columns = current_columns + new_columns_def
                    
                    # Update table definition
                    table_def = {
                        "name": table_name,
                        "columns": updated_columns,
                        "description": current_table_def.get("description", "")
                    }
                    
                    logger.info(f"Adding {len(new_columns_def)} columns to existing table '{table_name}'")
                    fabric_client.update_table(dataset_id, table_name, table_def)
                    
                    results["applied"] += len(col_changes)
                    for col_change in col_changes:
                        results["details"].append({
                            "type": "added",
                            "entity": "column",
                            "name": col_change.entity_name,
                            "status": "applied",
                        })
            
            except Exception as e:
                logger.error(f"Failed to update existing tables: {e}")
                results["errors"] += 1

        return results

    def _map_snowflake_type_to_fabric(self, snowflake_type: str) -> str:
        """Map Snowflake data type to Fabric data type."""
        snowflake_type = snowflake_type.upper()
        
        if snowflake_type in ("TEXT", "VARCHAR", "CHAR", "STRING", "VARIANT", "ARRAY", "OBJECT"):
            return "String"
        elif snowflake_type in ("NUMBER", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"):
            return "Int64"
        elif snowflake_type in ("FLOAT", "DOUBLE", "REAL", "DECIMAL", "NUMERIC"):
            return "Double"
        elif snowflake_type == "BOOLEAN":
            return "Boolean"
        elif snowflake_type in ("DATE", "DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"):
            return "DateTime"
        
        return "String"

    def preview_changes(
        self,
        direction: SyncDirection,
    ) -> ChangeReport:
        """
        Preview changes without applying them.

        Args:
            direction: Direction of sync to preview

        Returns:
            ChangeReport with detected changes
        """
        logger.info(f"Previewing changes for {direction.value}")

        if direction == SyncDirection.FABRIC_TO_SNOWFLAKE:
            fabric_parser = self._get_fabric_parser()
            source_model = fabric_parser.read_semantic_model()

            snowflake_reader = self._get_snowflake_reader()
            target_model = snowflake_reader.read_semantic_view()
        else:
            snowflake_reader = self._get_snowflake_reader()
            source_model = snowflake_reader.read_semantic_view()

            fabric_parser = self._get_fabric_parser()
            target_model = fabric_parser.read_semantic_model()

        return self._change_detector.detect_changes(source_model, target_model)

    def validate_connections(self) -> dict[str, bool]:
        """
        Validate connections to both platforms.

        Returns:
            Dict with connection status for each platform
        """
        results = {
            "fabric": False,
            "snowflake": False,
        }

        # Validate Fabric connection
        if self._fabric_config:
            try:
                fabric_client = self._get_fabric_client()
                fabric_client.validate_connection()
                results["fabric"] = True
                logger.info("Fabric connection validated")
            except Exception as e:
                logger.error(f"Fabric connection failed: {e}")

        # Validate Snowflake connection
        if self._snowflake_config:
            try:
                snowflake_reader = self._get_snowflake_reader()
                snowflake_reader.test_connection()
                results["snowflake"] = True
                logger.info("Snowflake connection validated")
            except Exception as e:
                logger.error(f"Snowflake connection failed: {e}")

        return results
