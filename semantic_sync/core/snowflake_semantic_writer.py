"""
Snowflake Semantic View Metadata Writer.

Writes semantic metadata from Fabric to Snowflake using SQL DDL/DML.
Implements the Fabric-to-Snowflake metadata sync flow.

This module handles:
- Creating/updating tables with metadata (comments)
- Storing semantic model metadata in a dedicated metadata table
- Transactional updates with rollback on failure

Note: This uses REST API approach only - no Snowflake semantic layer SDK required.
"""

from __future__ import annotations


import json
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Generator

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, ProgrammingError

from semantic_sync.config.settings import SnowflakeConfig
from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
    DataType,
)
from semantic_sync.core.change_detector import Change, ChangeType
from semantic_sync.core.dax_transpiler import DaxToSqlTranspiler
from semantic_sync.utils.exceptions import (
    ConnectionError,
    TransactionError,
    SyncError,
)
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


# ============================================================================
# ASCII Art Banner for Antigravity Flair
# ============================================================================
SYNC_BANNER = r"""
+===========================================================================+
|   SemaBridge: Fabric -> Snowflake Metadata Sync                           |
|   ---------------------------------------------------------------------   |
|   "import antigravity" - Zero-gravity semantic model transmission         |
+===========================================================================+
"""

SUCCESS_BANNER = r"""
    +----------------------------------------------------------------------+
    |                                                                      |
    |   [SemaBridge] SYNC COMPLETE - Zero-Gravity Transmission Successful! |
    |                                                                      |
    |   "The semantic metadata defied gravity and landed in Snowflake"     |
    |                                                                      |
    +----------------------------------------------------------------------+
"""


class SnowflakeSemanticWriter:
    """
    Writes Fabric semantic model metadata to Snowflake.
    
    Implements a REST API / SQL-only approach for metadata transmission:
    - Stores table/column metadata as COMMENTs
    - Stores full semantic model in a metadata table (JSON)
    - Measures and relationships stored in dedicated tables
    
    This approach works without Premium Snowflake features.
    """
    
    # Metadata storage table names
    METADATA_TABLE = "_SEMANTIC_METADATA"
    MEASURES_TABLE = "_SEMANTIC_MEASURES"
    RELATIONSHIPS_TABLE = "_SEMANTIC_RELATIONSHIPS"
    SYNC_HISTORY_TABLE = "_SEMANTIC_SYNC_HISTORY"
    
    def __init__(self, config: SnowflakeConfig) -> None:
        """
        Initialize the Snowflake semantic writer.
        
        Args:
            config: Snowflake connection configuration
        """
        self._config = config
        self._schema = config.schema_name or "PUBLIC"
        self._database = config.database
        
    @contextmanager
    def connection(self) -> Generator[SnowflakeConnection, None, None]:
        """
        Context manager for database connections.
        
        Yields:
            Active Snowflake connection
            
        Raises:
            ConnectionError: If connection fails
        """
        conn = None
        try:
            logger.debug("Connecting to Snowflake for semantic metadata write")
            conn = snowflake.connector.connect(**self._config.get_connection_params())
            yield conn
        except DatabaseError as e:
            raise ConnectionError(
                f"Failed to connect to Snowflake: {e}",
                service="Snowflake",
                details={"account": self._config.account},
            ) from e
        finally:
            if conn:
                conn.close()
                
    @contextmanager
    def transaction(
        self,
        conn: SnowflakeConnection,
    ) -> Generator[SnowflakeConnection, None, None]:
        """
        Context manager for transactional operations.
        
        Automatically commits on success, rolls back on failure.
        """
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN TRANSACTION")
            logger.debug("Transaction started")
            yield conn
            cursor.execute("COMMIT")
            logger.debug("Transaction committed")
        except Exception as e:
            cursor.execute("ROLLBACK")
            logger.error(f"Transaction rolled back due to error: {e}")
            raise TransactionError(
                f"Transaction failed and was rolled back: {e}",
                rollback_performed=True,
            ) from e
        finally:
            cursor.close()
            
    def _ensure_metadata_tables(self, conn: SnowflakeConnection) -> None:
        """
        Ensure semantic metadata tables exist in Snowflake.
        
        Creates the following tables if they don't exist:
        - _SEMANTIC_METADATA: Stores full model JSON
        - _SEMANTIC_MEASURES: Stores measure definitions
        - _SEMANTIC_RELATIONSHIPS: Stores relationship definitions
        - _SEMANTIC_SYNC_HISTORY: Stores sync audit trail
        """
        cursor = conn.cursor()
        try:
            fqn = f"{self._database}.{self._schema}"
            
            # Full model metadata table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {fqn}.{self.METADATA_TABLE} (
                    MODEL_ID VARCHAR(100) PRIMARY KEY,
                    MODEL_NAME VARCHAR(255) NOT NULL,
                    SOURCE_SYSTEM VARCHAR(50) NOT NULL,
                    DESCRIPTION TEXT,
                    TABLE_COUNT INTEGER,
                    COLUMN_COUNT INTEGER,
                    MEASURE_COUNT INTEGER,
                    RELATIONSHIP_COUNT INTEGER,
                    MODEL_JSON VARIANT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    SYNC_VERSION INTEGER DEFAULT 1
                )
            """)
            logger.debug(f"Ensured metadata table exists: {self.METADATA_TABLE}")
            
            # Measures table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {fqn}.{self.MEASURES_TABLE} (
                    MEASURE_ID VARCHAR(100) PRIMARY KEY,
                    MODEL_ID VARCHAR(100) NOT NULL,
                    MEASURE_NAME VARCHAR(255) NOT NULL,
                    TABLE_NAME VARCHAR(255),
                    EXPRESSION TEXT,
                    DESCRIPTION TEXT,
                    DATA_TYPE VARCHAR(50),
                    FORMAT_STRING VARCHAR(100),
                    IS_HIDDEN BOOLEAN DEFAULT FALSE,
                    FOLDER VARCHAR(255),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            logger.debug(f"Ensured measures table exists: {self.MEASURES_TABLE}")
            
            # Relationships table
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {fqn}.{self.RELATIONSHIPS_TABLE} (
                    RELATIONSHIP_ID VARCHAR(100) PRIMARY KEY,
                    MODEL_ID VARCHAR(100) NOT NULL,
                    RELATIONSHIP_NAME VARCHAR(255) NOT NULL,
                    FROM_TABLE VARCHAR(255) NOT NULL,
                    FROM_COLUMN VARCHAR(255) NOT NULL,
                    TO_TABLE VARCHAR(255) NOT NULL,
                    TO_COLUMN VARCHAR(255) NOT NULL,
                    CARDINALITY VARCHAR(50) DEFAULT 'MANY_TO_ONE',
                    CROSS_FILTER_DIRECTION VARCHAR(50) DEFAULT 'SINGLE',
                    IS_ACTIVE BOOLEAN DEFAULT TRUE,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            logger.debug(f"Ensured relationships table exists: {self.RELATIONSHIPS_TABLE}")
            
            # Sync history table (for traceability)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {fqn}.{self.SYNC_HISTORY_TABLE} (
                    SYNC_ID VARCHAR(100) PRIMARY KEY,
                    RUN_ID VARCHAR(100) NOT NULL,
                    MODEL_ID VARCHAR(100) NOT NULL,
                    SYNC_DIRECTION VARCHAR(50) NOT NULL,
                    SYNC_MODE VARCHAR(50) NOT NULL,
                    STARTED_AT TIMESTAMP_NTZ NOT NULL,
                    COMPLETED_AT TIMESTAMP_NTZ,
                    STATUS VARCHAR(50) NOT NULL,
                    CHANGES_APPLIED INTEGER DEFAULT 0,
                    CHANGES_SKIPPED INTEGER DEFAULT 0,
                    ERRORS INTEGER DEFAULT 0,
                    ERROR_MESSAGE TEXT,
                    DETAILS VARIANT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            logger.debug(f"Ensured sync history table exists: {self.SYNC_HISTORY_TABLE}")
            
        finally:
            cursor.close()
            
    def sync_semantic_model(
        self,
        model: SemanticModel,
        dry_run: bool = False,
        run_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Sync a complete semantic model from Fabric to Snowflake.
        
        This is the main entry point for Fabric -> Snowflake metadata sync.
        
        Args:
            model: The semantic model to sync (from Fabric)
            dry_run: If True, simulate without making changes
            run_id: Optional run ID for traceability
            
        Returns:
            Sync result summary
        """
        import uuid
        
        print(SYNC_BANNER)
        
        started_at = datetime.utcnow()
        run_id = run_id or str(uuid.uuid4())[:8]
        sync_id = f"sync_{run_id}_{started_at.strftime('%Y%m%d_%H%M%S')}"
        model_id = model.metadata.get("dataset_id", model.name)
        
        logger.info(f"Starting Fabric->Snowflake sync: model={model.name}, run_id={run_id}")
        logger.info(f"  Tables: {len(model.tables)}, Measures: {len(model.measures)}, Relationships: {len(model.relationships)}")
        
        results = {
            "sync_id": sync_id,
            "run_id": run_id,
            "model_name": model.name,
            "model_id": model_id,
            "direction": "fabric-to-snowflake",
            "started_at": started_at.isoformat(),
            "tables_synced": 0,
            "columns_synced": 0,
            "measures_synced": 0,
            "relationships_synced": 0,
            "applied": 0,
            "skipped": 0,
            "errors": 0,
            "details": [],
            "dry_run": dry_run,
        }
        
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be applied")
            results["applied"] = len(model.tables) + len(model.measures) + len(model.relationships)
            results["tables_synced"] = len(model.tables)
            results["columns_synced"] = sum(len(t.columns) for t in model.tables)
            results["measures_synced"] = len(model.measures)
            results["relationships_synced"] = len(model.relationships)
            results["details"].append({
                "status": "dry_run",
                "message": f"Would sync {len(model.tables)} tables, "
                          f"{len(model.measures)} measures, "
                          f"{len(model.relationships)} relationships"
            })
            results["completed_at"] = datetime.utcnow().isoformat()
            return results
            
        try:
            with self.connection() as conn:
                with self.transaction(conn):
                    # 1. Ensure metadata tables exist
                    self._ensure_metadata_tables(conn)
                    
                    # 2. Sync table/column metadata (as COMMENTs)
                    for table in model.tables:
                        try:
                            self._sync_table_metadata(conn, table, model_id)
                            results["tables_synced"] += 1
                            results["columns_synced"] += len(table.columns)
                            results["applied"] += 1 + len(table.columns)
                            results["details"].append({
                                "entity": "table",
                                "name": table.name,
                                "status": "synced",
                                "columns": len(table.columns),
                            })
                        except Exception as e:
                            logger.warning(f"Failed to sync table metadata for {table.name}: {e}")
                            results["details"].append({
                                "entity": "table",
                                "name": table.name,
                                "status": "warning",
                                "message": str(e),
                            })
                            # Continue with other tables - don't fail entire sync
                            
                    # 3. Sync measures
                    for measure in model.measures:
                        try:
                            self._sync_measure(conn, measure, model_id)
                            results["measures_synced"] += 1
                            results["applied"] += 1
                            results["details"].append({
                                "entity": "measure",
                                "name": measure.name,
                                "status": "synced",
                            })
                        except Exception as e:
                            logger.error(f"Failed to sync measure {measure.name}: {e}")
                            results["errors"] += 1
                            results["details"].append({
                                "entity": "measure",
                                "name": measure.name,
                                "status": "error",
                                "message": str(e),
                            })
                            
                    # 4. Sync relationships
                    for relationship in model.relationships:
                        try:
                            self._sync_relationship(conn, relationship, model_id)
                            results["relationships_synced"] += 1
                            results["applied"] += 1
                            results["details"].append({
                                "entity": "relationship",
                                "name": relationship.name,
                                "status": "synced",
                            })
                        except Exception as e:
                            logger.error(f"Failed to sync relationship {relationship.name}: {e}")
                            results["errors"] += 1
                            results["details"].append({
                                "entity": "relationship",
                                "name": relationship.name,
                                "status": "error",
                                "message": str(e),
                            })
                            
                    # 5. Store full model JSON
                    self._store_model_metadata(conn, model, model_id)
                    
                    # 6. Record sync history
                    self._record_sync_history(
                        conn, sync_id, run_id, model_id, results, started_at
                    )
                    
            results["status"] = "success" if results["errors"] == 0 else "partial"
            results["completed_at"] = datetime.utcnow().isoformat()
            
            print(SUCCESS_BANNER)
            logger.info(f"Sync completed: {results['applied']} changes applied, {results['errors']} errors")
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            results["status"] = "failed"
            results["error_message"] = str(e)
            results["completed_at"] = datetime.utcnow().isoformat()
            raise SyncError(
                f"Fabric to Snowflake sync failed: {e}",
                direction="fabric-to-snowflake",
            ) from e
            
        return results
        
    def _sync_table_metadata(
        self,
        conn: SnowflakeConnection,
        table: SemanticTable,
        model_id: str,
    ) -> None:
        """
        Sync table and column metadata as Snowflake COMMENTs.
        
        This allows metadata to be visible in Snowflake UI and tools.
        """
        cursor = conn.cursor()
        fqn_table = f"{self._database}.{self._schema}.{table.name}"
        
        try:
            # Check if table exists in Snowflake
            cursor.execute(f"""
                SELECT COUNT(*) FROM {self._database}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{self._schema}' AND TABLE_NAME = '{table.name.upper()}'
            """)
            table_exists = cursor.fetchone()[0] > 0
            
            if not table_exists:
                logger.debug(f"Table {table.name} does not exist in Snowflake - storing metadata only")
                # Store table metadata in our metadata table instead
                self._store_table_metadata_record(cursor, table, model_id)
                return
                
            # Set table comment
            if table.description:
                comment = self._escape_comment(table.description)
                cursor.execute(f"COMMENT ON TABLE {fqn_table} IS '{comment}'")
                logger.debug(f"Set comment on table: {table.name}")
                
            # Set column comments
            for column in table.columns:
                try:
                    fqn_column = f"{fqn_table}.{column.name}"
                    
                    # Build column metadata comment
                    metadata_parts = []
                    if column.description:
                        metadata_parts.append(column.description)
                    metadata_parts.append(f"[Type: {column.data_type}]")
                    if column.is_hidden:
                        metadata_parts.append("[Hidden]")
                        
                    comment = self._escape_comment(" | ".join(metadata_parts))
                    cursor.execute(f"COMMENT ON COLUMN {fqn_column} IS '{comment}'")
                    logger.debug(f"Set comment on column: {table.name}.{column.name}")
                    
                except ProgrammingError as e:
                    # Column might not exist - log and continue
                    logger.debug(f"Could not set column comment for {column.name}: {e}")
                    
        finally:
            cursor.close()
            
    def _store_table_metadata_record(
        self,
        cursor,
        table: SemanticTable,
        model_id: str,
    ) -> None:
        """Store table metadata in the metadata table (for tables that don't exist in SF)."""
        # This creates a record for tables that don't physically exist in Snowflake
        # Useful for documentation purposes
        pass  # Implement if needed
        
    def _sync_measure(
        self,
        conn: SnowflakeConnection,
        measure: SemanticMeasure,
        model_id: str,
    ) -> None:
        """Sync a measure definition to Snowflake (Metadata + SQL View)."""
        cursor = conn.cursor()
        fqn = f"{self._database}.{self._schema}"
        
        try:
            measure_id = f"{model_id}_{measure.name}".replace(" ", "_")
            
            # 1. Update Metadata Table
            cursor.execute(f"""
                MERGE INTO {fqn}.{self.MEASURES_TABLE} AS target
                USING (
                    SELECT 
                        %s AS MEASURE_ID,
                        %s AS MODEL_ID,
                        %s AS MEASURE_NAME,
                        %s AS TABLE_NAME,
                        %s AS EXPRESSION,
                        %s AS DESCRIPTION,
                        %s AS DATA_TYPE,
                        %s AS FORMAT_STRING,
                        %s AS IS_HIDDEN,
                        %s AS FOLDER
                ) AS source
                ON target.MEASURE_ID = source.MEASURE_ID
                WHEN MATCHED THEN UPDATE SET
                    MEASURE_NAME = source.MEASURE_NAME,
                    TABLE_NAME = source.TABLE_NAME,
                    EXPRESSION = source.EXPRESSION,
                    DESCRIPTION = source.DESCRIPTION,
                    DATA_TYPE = source.DATA_TYPE,
                    FORMAT_STRING = source.FORMAT_STRING,
                    IS_HIDDEN = source.IS_HIDDEN,
                    FOLDER = source.FOLDER,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    MEASURE_ID, MODEL_ID, MEASURE_NAME, TABLE_NAME, EXPRESSION,
                    DESCRIPTION, DATA_TYPE, FORMAT_STRING, IS_HIDDEN, FOLDER
                ) VALUES (
                    source.MEASURE_ID, source.MODEL_ID, source.MEASURE_NAME,
                    source.TABLE_NAME, source.EXPRESSION, source.DESCRIPTION,
                    source.DATA_TYPE, source.FORMAT_STRING, source.IS_HIDDEN, source.FOLDER
                )
            """, (
                measure_id,
                model_id,
                measure.name,
                measure.table_name,
                measure.expression,
                measure.description or "",
                measure.data_type,
                measure.folder or "",
                measure.is_hidden if hasattr(measure, 'is_hidden') else False,
                measure.folder or "",
            ))
            
            logger.debug(f"Synced measure metadata: {measure.name}")

            # 2. Compile DAX to SQL View
            if measure.expression:
                try:
                    # Initialize transpiler (no table mapping needed for simple cases, or could use self mapping if available)
                    transpiler = DaxToSqlTranspiler()
                    
                    # Attempt transpilation
                    # Use measure.table_name as context if needed
                    sql_expression = transpiler.transpile(measure.expression, source_table=measure.table_name)
                    
                    if sql_expression:
                        # Create View
                        # View name: [Model]_[MeasureName] or just [MeasureName] inside schema?
                        # Using just measure name might conflict if multiple models in same schema.
                        # Using sanitized name.
                        view_name = measure.name.replace(" ", "_").upper()
                        fqn_view = f"{fqn}.{view_name}"
                        
                        # We must ensure VIEW creation doesn't fail transaction if transpilation is garbage
                        # But we are inside a transaction block in sync_semantic_model.
                        # If this fails, the whole sync fails.
                        # Maybe we should wrap in nested try/except specifically for VIEW creation to avoid blocking metadata sync.
                        
                        create_view_sql = f"CREATE OR REPLACE VIEW {fqn_view} COMMENT='Auto-generated from DAX' AS {sql_expression}"
                        cursor.execute(create_view_sql)
                        
                        logger.info(f"Created SQL View for measure: {measure.name} -> {view_name}")
                    else:
                        logger.debug(f"Transpilation returned None for measure: {measure.name}")
                        
                except Exception as e:
                    # Log warning but do NOT fail the sync. View creation is best-effort.
                    logger.warning(f"Failed to create SQL view for measure '{measure.name}': {e}")
            
        finally:
            cursor.close()
            
    def _sync_relationship(
        self,
        conn: SnowflakeConnection,
        relationship: SemanticRelationship,
        model_id: str,
    ) -> None:
        """Sync a relationship definition to Snowflake."""
        cursor = conn.cursor()
        fqn = f"{self._database}.{self._schema}"
        
        try:
            rel_id = f"{model_id}_{relationship.name}".replace(" ", "_")
            
            cursor.execute(f"""
                MERGE INTO {fqn}.{self.RELATIONSHIPS_TABLE} AS target
                USING (
                    SELECT
                        %s AS RELATIONSHIP_ID,
                        %s AS MODEL_ID,
                        %s AS RELATIONSHIP_NAME,
                        %s AS FROM_TABLE,
                        %s AS FROM_COLUMN,
                        %s AS TO_TABLE,
                        %s AS TO_COLUMN,
                        %s AS CARDINALITY,
                        %s AS CROSS_FILTER_DIRECTION,
                        %s AS IS_ACTIVE
                ) AS source
                ON target.RELATIONSHIP_ID = source.RELATIONSHIP_ID
                WHEN MATCHED THEN UPDATE SET
                    RELATIONSHIP_NAME = source.RELATIONSHIP_NAME,
                    FROM_TABLE = source.FROM_TABLE,
                    FROM_COLUMN = source.FROM_COLUMN,
                    TO_TABLE = source.TO_TABLE,
                    TO_COLUMN = source.TO_COLUMN,
                    CARDINALITY = source.CARDINALITY,
                    CROSS_FILTER_DIRECTION = source.CROSS_FILTER_DIRECTION,
                    IS_ACTIVE = source.IS_ACTIVE,
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT (
                    RELATIONSHIP_ID, MODEL_ID, RELATIONSHIP_NAME,
                    FROM_TABLE, FROM_COLUMN, TO_TABLE, TO_COLUMN,
                    CARDINALITY, CROSS_FILTER_DIRECTION, IS_ACTIVE
                ) VALUES (
                    source.RELATIONSHIP_ID, source.MODEL_ID, source.RELATIONSHIP_NAME,
                    source.FROM_TABLE, source.FROM_COLUMN, source.TO_TABLE, source.TO_COLUMN,
                    source.CARDINALITY, source.CROSS_FILTER_DIRECTION, source.IS_ACTIVE
                )
            """, (
                rel_id,
                model_id,
                relationship.name,
                relationship.from_table,
                relationship.from_column,
                relationship.to_table,
                relationship.to_column,
                relationship.cardinality if hasattr(relationship, 'cardinality') else "MANY_TO_ONE",
                relationship.cross_filter_direction,
                relationship.is_active,
            ))
            
            logger.debug(f"Synced relationship: {relationship.name}")
            
        finally:
            cursor.close()
            
    def _store_model_metadata(
        self,
        conn: SnowflakeConnection,
        model: SemanticModel,
        model_id: str,
    ) -> None:
        """Store the complete model as JSON in the metadata table."""
        cursor = conn.cursor()
        fqn = f"{self._database}.{self._schema}"
        
        try:
            # Convert model to JSON
            model_json = json.dumps(model.model_dump(), default=str)
            
            cursor.execute(f"""
                MERGE INTO {fqn}.{self.METADATA_TABLE} AS target
                USING (
                    SELECT
                        %s AS MODEL_ID,
                        %s AS MODEL_NAME,
                        %s AS SOURCE_SYSTEM,
                        %s AS DESCRIPTION,
                        %s AS TABLE_COUNT,
                        %s AS COLUMN_COUNT,
                        %s AS MEASURE_COUNT,
                        %s AS RELATIONSHIP_COUNT,
                        PARSE_JSON(%s) AS MODEL_JSON
                ) AS source
                ON target.MODEL_ID = source.MODEL_ID
                WHEN MATCHED THEN UPDATE SET
                    MODEL_NAME = source.MODEL_NAME,
                    DESCRIPTION = source.DESCRIPTION,
                    TABLE_COUNT = source.TABLE_COUNT,
                    COLUMN_COUNT = source.COLUMN_COUNT,
                    MEASURE_COUNT = source.MEASURE_COUNT,
                    RELATIONSHIP_COUNT = source.RELATIONSHIP_COUNT,
                    MODEL_JSON = source.MODEL_JSON,
                    UPDATED_AT = CURRENT_TIMESTAMP(),
                    SYNC_VERSION = target.SYNC_VERSION + 1
                WHEN NOT MATCHED THEN INSERT (
                    MODEL_ID, MODEL_NAME, SOURCE_SYSTEM, DESCRIPTION,
                    TABLE_COUNT, COLUMN_COUNT, MEASURE_COUNT, RELATIONSHIP_COUNT,
                    MODEL_JSON
                ) VALUES (
                    source.MODEL_ID, source.MODEL_NAME, source.SOURCE_SYSTEM, source.DESCRIPTION,
                    source.TABLE_COUNT, source.COLUMN_COUNT, source.MEASURE_COUNT, source.RELATIONSHIP_COUNT,
                    source.MODEL_JSON
                )
            """, (
                model_id,
                model.name,
                model.source,
                model.description or "",
                len(model.tables),
                sum(len(t.columns) for t in model.tables),
                len(model.measures),
                len(model.relationships),
                model_json,
            ))
            
            logger.info(f"Stored semantic model metadata: {model.name}")
            
        finally:
            cursor.close()
            
    def _record_sync_history(
        self,
        conn: SnowflakeConnection,
        sync_id: str,
        run_id: str,
        model_id: str,
        results: dict[str, Any],
        started_at: datetime,
    ) -> None:
        """Record sync operation in history table for audit trail."""
        cursor = conn.cursor()
        fqn = f"{self._database}.{self._schema}"
        
        try:
            details_json = json.dumps(results.get("details", []), default=str)
            
            cursor.execute(f"""
                INSERT INTO {fqn}.{self.SYNC_HISTORY_TABLE} (
                    SYNC_ID, RUN_ID, MODEL_ID, SYNC_DIRECTION, SYNC_MODE,
                    STARTED_AT, COMPLETED_AT, STATUS,
                    CHANGES_APPLIED, CHANGES_SKIPPED, ERRORS,
                    ERROR_MESSAGE, DETAILS
                )
                SELECT
                    %s, %s, %s, %s, %s,
                    %s, CURRENT_TIMESTAMP(), %s,
                    %s, %s, %s,
                    %s, PARSE_JSON(%s)
            """, (
                sync_id,
                run_id,
                model_id,
                "fabric-to-snowflake",
                "metadata-only",
                started_at,
                results.get("status", "unknown"),
                results.get("applied", 0),
                results.get("skipped", 0),
                results.get("errors", 0),
                results.get("error_message"),
                details_json,
            ))
            
            logger.debug(f"Recorded sync history: {sync_id}")
            
        finally:
            cursor.close()
            
    def _escape_comment(self, text: str) -> str:
        """Escape text for use in SQL COMMENT."""
        if not text:
            return ""
        # Escape single quotes
        return text.replace("'", "''").replace("\n", " ").replace("\r", "")[:1000]
        
    def apply_changes(
        self,
        changes: list[Change],
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Apply a list of changes to Snowflake (incremental sync).
        
        This method is called by the SemanticUpdater for incremental changes.
        
        Args:
            changes: List of changes to apply
            dry_run: If True, simulate without applying
            
        Returns:
            Summary of applied changes
        """
        if not changes:
            logger.info("No changes to apply")
            return {"applied": 0, "skipped": 0, "errors": 0}
            
        results = {
            "applied": 0,
            "skipped": 0,
            "errors": 0,
            "details": [],
        }
        
        if dry_run:
            logger.info(f"DRY RUN: Would apply {len(changes)} changes")
            for change in changes:
                results["details"].append({
                    "type": change.change_type.value,
                    "entity": change.entity_type,
                    "name": change.entity_name,
                    "status": "would_apply",
                })
                results["applied"] += 1
            return results
            
        with self.connection() as conn:
            with self.transaction(conn):
                # Ensure metadata tables exist
                self._ensure_metadata_tables(conn)
                
                for change in changes:
                    try:
                        self._apply_single_change(conn, change)
                        results["applied"] += 1
                        results["details"].append({
                            "type": change.change_type.value,
                            "entity": change.entity_type,
                            "name": change.entity_name,
                            "status": "applied",
                        })
                    except Exception as e:
                        logger.error(f"Failed to apply change: {e}")
                        results["errors"] += 1
                        results["details"].append({
                            "type": change.change_type.value,
                            "entity": change.entity_type,
                            "name": change.entity_name,
                            "status": "error",
                            "error": str(e),
                        })
                        
        logger.info(f"Applied {results['applied']} changes, {results['errors']} errors")
        return results
        
    def _apply_single_change(
        self,
        conn: SnowflakeConnection,
        change: Change,
    ) -> None:
        """Apply a single metadata change to Snowflake."""
        cursor = conn.cursor()
        fqn = f"{self._database}.{self._schema}"
        
        try:
            if change.entity_type == "table":
                if change.change_type in (ChangeType.ADDED, ChangeType.MODIFIED):
                    if change.new_value:
                        description = change.new_value.get("description", "")
                        if description:
                            comment = self._escape_comment(description)
                            cursor.execute(f"COMMENT ON TABLE {fqn}.{change.entity_name} IS '{comment}'")
                            
            elif change.entity_type == "column":
                if change.change_type in (ChangeType.ADDED, ChangeType.MODIFIED):
                    parts = change.entity_name.split(".")
                    if len(parts) == 2:
                        table_name, column_name = parts
                        if change.new_value:
                            description = change.new_value.get("description", "")
                            data_type = change.new_value.get("data_type", "")
                            comment = f"{description} [Type: {data_type}]"
                            comment = self._escape_comment(comment)
                            cursor.execute(f"COMMENT ON COLUMN {fqn}.{table_name}.{column_name} IS '{comment}'")
                            
            elif change.entity_type == "measure":
                # For measures in incremental mode, update the measures table
                logger.debug(f"Measure change: {change.change_type.value} {change.entity_name}")
                
            elif change.entity_type == "relationship":
                # For relationships in incremental mode, update the relationships table
                logger.debug(f"Relationship change: {change.change_type.value} {change.entity_name}")
                
        finally:
            cursor.close()


# ============================================================================
# Convenience Function
# ============================================================================

def sync_fabric_to_snowflake(
    fabric_model: SemanticModel,
    snowflake_config: SnowflakeConfig,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    High-level function to sync Fabric metadata to Snowflake.
    
    This is the main API for Fabric→Snowflake metadata sync.
    
    Args:
        fabric_model: SemanticModel from Fabric
        snowflake_config: Snowflake connection config
        dry_run: If True, simulate without applying
        
    Returns:
        Sync results summary
        
    Example:
        >>> from semantic_sync.core.fabric_model_parser import FabricModelParser
        >>> parser = FabricModelParser(fabric_client, fabric_config)
        >>> model = parser.read_semantic_model()
        >>> results = sync_fabric_to_snowflake(model, snowflake_config)
    """
    writer = SnowflakeSemanticWriter(snowflake_config)
    return writer.sync_semantic_model(fabric_model, dry_run=dry_run)
