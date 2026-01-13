"""
Semantic model updater for synchronization operations.

Orchestrates the synchronization process between Fabric and Snowflake,
applying detected changes to the target system.
"""

from __future__ import annotations


from datetime import datetime
from enum import Enum
from typing import Any
from dataclasses import dataclass, field

from semantic_sync.core.models import SemanticModel
from semantic_sync.core.change_detector import ChangeDetector, ChangeReport, Change, ChangeType
from semantic_sync.core.snowflake_reader import SnowflakeReader
from semantic_sync.core.snowflake_writer import SnowflakeWriter
from semantic_sync.core.snowflake_semantic_writer import SnowflakeSemanticWriter
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
        self._snowflake_semantic_writer: SnowflakeSemanticWriter | None = None

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

    def _get_snowflake_semantic_writer(self) -> SnowflakeSemanticWriter:
        """Get or create Snowflake semantic metadata writer (REST API approach)."""
        if self._snowflake_semantic_writer is None:
            if self._snowflake_config is None:
                raise ValidationError(
                    "Snowflake configuration is required",
                    details={"missing": "snowflake_config"},
                )
            self._snowflake_semantic_writer = SnowflakeSemanticWriter(self._snowflake_config)
        return self._snowflake_semantic_writer

    def sync_all_workspace_datasets(
        self,
        mode: SyncMode = SyncMode.METADATA_ONLY,
        dry_run: bool = False,
    ) -> list[SyncResult]:
        """
        Sync ALL datasets in the configured workspace to Snowflake.

        Args:
            mode: Sync mode
            dry_run: Simulate only

        Returns:
            List of SyncResult objects for each dataset
        """
        logger.info(f"Starting workspace sync (Workspace ID: {self._fabric_config.workspace_id})")
        
        client = self._get_fabric_client()
        try:
            datasets = client.list_workspace_datasets()
        except Exception as e:
            logger.error(f"Failed to list datasets in workspace: {e}")
            raise

        logger.info(f"Found {len(datasets)} datasets provided by Fabric")
        
        results = []
        for ds in datasets:
            ds_name = ds.get("name", "Unknown")
            ds_id = ds.get("id")
            
            # Skip invalid or unsupported datasets if necessary
            # e.g. check configuredBy or specific properties
            
            logger.info(f"Syncing dataset: {ds_name} ({ds_id})")
            print(f"\nProcessing dataset: {ds_name}...")
            
            try:
                result = self.sync(
                    direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                    mode=mode,
                    dry_run=dry_run,
                    dataset_id=ds_id,
                )
                results.append(result)
                
                status = "[OK]" if result.success else "[FAIL]"
                print(f"  {status} {result.error_message or 'Success'}")
                
            except Exception as e:
                logger.error(f"Sync failed for dataset {ds_name}: {e}")
                print(f"  [ERROR] {e}")
                results.append(SyncResult(
                    success=False,
                    direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                    mode=mode,
                    changes_applied=0,
                    changes_skipped=0,
                    errors=1,
                    started_at=datetime.utcnow(),
                    completed_at=datetime.utcnow(),
                    source_model=ds_name,
                    dry_run=dry_run,
                    error_message=str(e),
                ))
        
        return results

    def sync(
        self,
        direction: SyncDirection,
        mode: SyncMode = SyncMode.INCREMENTAL,
        dry_run: bool = False,
        validate_only: bool = False,
        dataset_id: str | None = None,
    ) -> SyncResult:
        """
        Perform semantic model synchronization.

        Args:
            direction: Direction of sync (Fabric→Snowflake or Snowflake→Fabric)
            mode: Sync mode (full, incremental, metadata-only)
            dry_run: If True, simulate sync without applying changes
            validate_only: If True, only validate and return change report
            dataset_id: Optional dataset ID to sync (overrides config)

        Returns:
            SyncResult with operation details

        Raises:
            SyncError: If synchronization fails
        """
        started_at = datetime.utcnow()
        logger.info(
            f"Starting sync: {direction.value}, mode={mode.value}, "
            f"dry_run={dry_run}, validate_only={validate_only}, dataset_id={dataset_id}"
        )

        try:
            if direction == SyncDirection.FABRIC_TO_SNOWFLAKE:
                return self._sync_fabric_to_snowflake(
                    mode=mode,
                    dry_run=dry_run,
                    validate_only=validate_only,
                    started_at=started_at,
                    dataset_id=dataset_id,
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
        dataset_id: str | None = None,
    ) -> SyncResult:
        """
        Sync from Fabric to Snowflake (metadata-only, REST API approach).
        
        This method reads the semantic model from Fabric using REST API
        and writes the metadata to Snowflake using SQL (no XMLA required).
        """
        # Read source model from Fabric
        logger.info(f"Reading source model from Fabric via REST API{' (ID: ' + dataset_id + ')' if dataset_id else ''}")
        fabric_parser = self._get_fabric_parser()
        source_model = fabric_parser.read_semantic_model(dataset_id=dataset_id)
        
        logger.info(f"Fabric model loaded: {source_model.name}")
        logger.info(f"  Tables: {len(source_model.tables)}")
        logger.info(f"  Measures: {len(source_model.measures)}")
        logger.info(f"  Relationships: {len(source_model.relationships)}")

        if validate_only:
            # For validation, we still detect changes against current Snowflake state
            try:
                snowflake_reader = self._get_snowflake_reader()
                target_model = snowflake_reader.read_semantic_view()
                change_report = self._change_detector.detect_changes(source_model, target_model)
                
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
            except Exception as e:
                logger.warning(f"Could not read Snowflake for validation: {e}")
                # Continue without target comparison
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
                    target_model="unknown",
                    dry_run=True,
                    details=[{"status": "validation_only", "model": source_model.name}],
                )

        # For metadata-only or full sync, use the SnowflakeSemanticWriter
        # This writes the complete model metadata to Snowflake metadata tables
        if mode in (SyncMode.METADATA_ONLY, SyncMode.FULL):
            logger.info(f"Using SnowflakeSemanticWriter for {mode.value} sync")
            semantic_writer = self._get_snowflake_semantic_writer()
            
            # Perform full model sync to Snowflake metadata tables
            sync_result = semantic_writer.sync_semantic_model(
                model=source_model,
                dry_run=dry_run,
            )
            
            return SyncResult(
                success=sync_result.get("errors", 0) == 0,
                direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                mode=mode,
                changes_applied=sync_result.get("applied", 0),
                changes_skipped=sync_result.get("skipped", 0),
                errors=sync_result.get("errors", 0),
                started_at=started_at,
                completed_at=datetime.utcnow(),
                source_model=source_model.name,
                target_model=f"{self._snowflake_config.database}.{self._snowflake_config.schema_name}",
                dry_run=dry_run,
                details=sync_result.get("details", []),
            )
        
        # For incremental mode, detect and apply only changes
        logger.info("Incremental mode: detecting changes")
        try:
            snowflake_reader = self._get_snowflake_reader()
            target_model = snowflake_reader.read_semantic_view()
        except Exception as e:
            logger.warning(f"Could not read existing Snowflake model: {e}")
            # If we can't read target, treat as full sync
            logger.info("Falling back to full metadata sync")
            semantic_writer = self._get_snowflake_semantic_writer()
            sync_result = semantic_writer.sync_semantic_model(
                model=source_model,
                dry_run=dry_run,
            )
            
            return SyncResult(
                success=sync_result.get("errors", 0) == 0,
                direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                mode=SyncMode.FULL,
                changes_applied=sync_result.get("applied", 0),
                changes_skipped=sync_result.get("skipped", 0),
                errors=sync_result.get("errors", 0),
                started_at=started_at,
                completed_at=datetime.utcnow(),
                source_model=source_model.name,
                target_model=f"{self._snowflake_config.database}.{self._snowflake_config.schema_name}",
                dry_run=dry_run,
                details=sync_result.get("details", []),
            )

        # Detect changes
        change_report = self._change_detector.detect_changes(source_model, target_model)
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

        # Apply incremental changes using the semantic writer
        logger.info(f"Applying {len(changes_to_apply)} incremental changes to Snowflake")
        semantic_writer = self._get_snowflake_semantic_writer()
        apply_result = semantic_writer.apply_changes(changes_to_apply, dry_run=dry_run)

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

        Handles Push API limitations:
        - PUT works for updating existing tables (adding columns)
        - POST does NOT work for creating new tables after dataset creation
        - If new tables are needed, creates a new dataset with all required tables
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

        results = {
            "applied": 0,
            "skipped": 0,
            "errors": 0,
            "details": [],
        }

        fabric_client = self._get_fabric_client()
        dataset_id = self._fabric_config.dataset_id
        
        # Get existing tables in the target dataset
        existing_table_names = fabric_client.get_existing_table_names(dataset_id)
        logger.info(f"Existing tables in target dataset: {existing_table_names}")
        
        # Group changes by type
        tables_to_add = {}
        tables_to_update = {}
        columns_to_add_by_table = {}
        new_tables_needed = []
        
        for change in changes:
            try:
                if change.entity_type == "table":
                    if change.change_type == ChangeType.ADDED:
                        table_name = change.entity_name
                        if table_name in existing_table_names:
                            # Table exists - can update it
                            tables_to_update[table_name] = change
                        else:
                            # New table needed
                            tables_to_add[table_name] = change
                            new_tables_needed.append(table_name)
                    elif change.change_type == ChangeType.MODIFIED:
                        logger.debug(f"Table metadata change: {change.entity_name}")
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
                
                elif change.entity_type == "column":
                    if change.change_type == ChangeType.ADDED:
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
                    elif change.change_type == ChangeType.MODIFIED:
                        results["applied"] += 1
                    else:
                        results["skipped"] += 1
                
                else:
                    logger.warning(f"Entity type {change.entity_type} not supported")
                    results["skipped"] += 1

            except Exception as e:
                logger.error(f"Failed to process change: {e}")
                results["errors"] += 1
        
        # Check if we need to create new tables
        if new_tables_needed:
            logger.warning(
                f"Push API limitation: Cannot add new tables to existing dataset. "
                f"New tables needed: {new_tables_needed}"
            )
            
            # Strategy: Create a new dataset with all required tables
            # Build table definitions for the new dataset
            all_tables_def = []
            
            # Include existing tables (with their current columns)
            for table_name in existing_table_names:
                try:
                    # Get current table definition
                    current_tables = fabric_client.get_dataset_tables(dataset_id)
                    for t in current_tables:
                        if t.get("name") == table_name:
                            # Use existing columns or add placeholder
                            cols = t.get("columns", [{"name": "ID", "dataType": "Int64"}])
                            if not cols:
                                cols = [{"name": "ID", "dataType": "Int64"}]
                            all_tables_def.append({
                                "name": table_name,
                                "columns": cols,
                            })
                            break
                except Exception as e:
                    logger.error(f"Failed to get existing table definition: {e}")
            
            # Add new tables with their columns
            for table_name in new_tables_needed:
                table_columns = columns_to_add_by_table.get(table_name, [])
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
                    columns_def = [{"name": "ID", "dataType": "Int64"}]
                
                all_tables_def.append({
                    "name": table_name,
                    "columns": columns_def,
                })
            
            # Create new dataset
            try:
                import datetime
                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                new_dataset_name = f"SnowflakeSync_{timestamp}"
                
                logger.info(f"Creating new Push dataset '{new_dataset_name}' with {len(all_tables_def)} tables")
                new_dataset = fabric_client.create_push_dataset(
                    name=new_dataset_name,
                    tables=all_tables_def,
                )
                
                new_dataset_id = new_dataset.get("id")
                logger.info(f"[OK] Created new dataset: {new_dataset_name} (ID: {new_dataset_id})")
                print(f"\n[OK] Created new Push dataset: {new_dataset_name}")
                print(f"     New Dataset ID: {new_dataset_id}")
                print(f"     Tables created: {len(all_tables_def)}")
                print(f"\n     To use this dataset, update FABRIC_DATASET_ID in your .env file:")
                print(f"     FABRIC_DATASET_ID={new_dataset_id}\n")
                
                # Count successes
                results["applied"] += len(tables_to_add)
                for table_name in new_tables_needed:
                    results["details"].append({
                        "type": "added",
                        "entity": "table",
                        "name": table_name,
                        "status": "applied",
                        "note": f"Created in new dataset: {new_dataset_name}",
                    })
                    # Count columns for this table
                    for col_change in columns_to_add_by_table.get(table_name, []):
                        results["applied"] += 1
                        results["details"].append({
                            "type": "added",
                            "entity": "column",
                            "name": col_change.entity_name,
                            "status": "applied",
                        })
                
                # Remove processed tables from columns list
                for table_name in new_tables_needed:
                    if table_name in columns_to_add_by_table:
                        del columns_to_add_by_table[table_name]
                
            except Exception as e:
                logger.error(f"Failed to create new dataset: {e}")
                print(f"\n[ERROR] Failed to create new dataset: {e}")
                results["errors"] += len(new_tables_needed)
                for table_name in new_tables_needed:
                    results["details"].append({
                        "type": "added",
                        "entity": "table",
                        "name": table_name,
                        "status": "error",
                        "error": str(e),
                    })
        
        # Update existing tables with new columns
        if columns_to_add_by_table:
            try:
                current_tables = fabric_client.get_dataset_tables(dataset_id)
                current_tables_map = {t["name"]: t for t in current_tables}
                
                for table_name, col_changes in columns_to_add_by_table.items():
                    target_table_name = table_name
                    
                    if table_name not in current_tables_map:
                        # Try case-insensitive match
                        found_match = False
                        for existing_name in current_tables_map:
                            if existing_name.lower() == table_name.lower():
                                target_table_name = existing_name
                                found_match = True
                                logger.info(f"Case-insensitive match: '{table_name}' -> '{target_table_name}'")
                                break
                        
                        if not found_match:
                            # Table doesn't exist and wasn't created above - skip
                            logger.warning(f"Skipping columns for non-existent table: {table_name}")
                            for col_change in col_changes:
                                results["skipped"] += 1
                                results["details"].append({
                                    "type": "added",
                                    "entity": "column",
                                    "name": col_change.entity_name,
                                    "status": "skipped",
                                    "reason": f"Table {table_name} does not exist",
                                })
                            continue
                    
                    current_table_def = current_tables_map[target_table_name]
                    current_columns = current_table_def.get("columns", [])
                    
                    new_columns_def = []
                    for col_change in col_changes:
                        col_name = col_change.entity_name
                        if "." in col_name:
                            col_name = col_name.split(".")[-1]
                        
                        if any(c.get("name") == col_name for c in current_columns):
                            continue
                        
                        data_type = "String"
                        if col_change.new_value:
                            data_type = col_change.new_value.get("data_type", "String")
                        
                        new_columns_def.append({
                            "name": col_name,
                            "dataType": self._map_snowflake_type_to_fabric(data_type),
                        })
                    
                    if not new_columns_def:
                        continue
                    
                    updated_columns = current_columns + new_columns_def
                    table_def = {
                        "name": table_name,
                        "columns": updated_columns,
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
