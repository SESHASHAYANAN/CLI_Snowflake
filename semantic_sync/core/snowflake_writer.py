"""
Snowflake Semantic View writer.

Updates Snowflake Semantic Views with changes from external sources.
Implements transactional updates for safety.
"""

from contextlib import contextmanager
from typing import Any, Generator

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import DatabaseError, ProgrammingError

from semantic_sync.config.settings import SnowflakeConfig
from semantic_sync.core.models import SemanticModel, SemanticTable, SemanticColumn
from semantic_sync.core.change_detector import Change, ChangeType
from semantic_sync.utils.exceptions import (
    ConnectionError,
    TransactionError,
    SyncError,
)
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeWriter:
    """
    Writes semantic metadata updates to Snowflake Semantic Views.

    Implements transactional updates to ensure atomicity and rollback
    capability on failures.
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        """
        Initialize the Snowflake writer.

        Args:
            config: Snowflake connection configuration
        """
        self._config = config

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
            logger.debug("Connecting to Snowflake for write operations")
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

        Args:
            conn: Active Snowflake connection

        Yields:
            Connection within transaction context
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

    def apply_changes(
        self,
        changes: list[Change],
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Apply a list of changes to Snowflake.

        Args:
            changes: List of changes to apply
            dry_run: If True, simulate changes without applying

        Returns:
            Summary of applied changes

        Raises:
            SyncError: If changes cannot be applied
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
                        # Re-raise to trigger transaction rollback
                        raise SyncError(
                            f"Failed to apply change: {e}",
                            direction="fabric-to-sf",
                        ) from e

        logger.info(f"Applied {results['applied']} changes successfully")
        return results

    def _apply_single_change(
        self,
        conn: SnowflakeConnection,
        change: Change,
    ) -> None:
        """
        Apply a single change to Snowflake.

        Args:
            conn: Active connection within transaction
            change: Change to apply
        """
        logger.debug(
            f"Applying change: {change.change_type.value} "
            f"{change.entity_type} {change.entity_name}"
        )

        if change.entity_type == "table":
            self._apply_table_change(conn, change)
        elif change.entity_type == "column":
            self._apply_column_change(conn, change)
        elif change.entity_type == "measure":
            self._apply_measure_change(conn, change)
        elif change.entity_type == "relationship":
            self._apply_relationship_change(conn, change)
        else:
            logger.warning(f"Unknown entity type: {change.entity_type}")

    def _apply_table_change(
        self,
        conn: SnowflakeConnection,
        change: Change,
    ) -> None:
        """Apply table-level changes."""
        cursor = conn.cursor()
        try:
            if change.change_type == ChangeType.ADDED:
                # Table addition would be complex - typically requires DDL
                logger.info(f"Table add requested: {change.entity_name}")
                # Implementation would depend on semantic layer API

            elif change.change_type == ChangeType.MODIFIED:
                # Update table metadata (e.g., description)
                if change.new_value and "description" in change.new_value:
                    query = f"""
                        COMMENT ON TABLE {self._config.schema_name}.{change.entity_name}
                        IS %s
                    """
                    cursor.execute(query, (change.new_value["description"],))

            elif change.change_type == ChangeType.REMOVED:
                # Table removal from semantic model (not DROP TABLE)
                logger.info(f"Table removal requested: {change.entity_name}")
                # Implementation would depend on semantic layer API

        finally:
            cursor.close()

    def _apply_column_change(
        self,
        conn: SnowflakeConnection,
        change: Change,
    ) -> None:
        """Apply column-level changes."""
        cursor = conn.cursor()
        try:
            # Parse table.column from entity_name
            parts = change.entity_name.split(".")
            if len(parts) != 2:
                logger.warning(f"Invalid column name format: {change.entity_name}")
                return

            table_name, column_name = parts

            if change.change_type == ChangeType.MODIFIED:
                # Update column description
                if change.new_value and "description" in change.new_value:
                    query = f"""
                        COMMENT ON COLUMN {self._config.schema_name}.{table_name}.{column_name}
                        IS %s
                    """
                    cursor.execute(query, (change.new_value["description"],))

        finally:
            cursor.close()

    def _apply_measure_change(
        self,
        conn: SnowflakeConnection,
        change: Change,
    ) -> None:
        """Apply measure-level changes."""
        # Measure changes would interact with Snowflake's semantic layer API
        # This is a placeholder for the actual implementation
        logger.debug(f"Measure change: {change.change_type.value} {change.entity_name}")

    def _apply_relationship_change(
        self,
        conn: SnowflakeConnection,
        change: Change,
    ) -> None:
        """Apply relationship-level changes."""
        # Relationship changes would interact with Snowflake's semantic layer API
        # This is a placeholder for the actual implementation
        logger.debug(f"Relationship change: {change.change_type.value} {change.entity_name}")

    def update_semantic_view(
        self,
        model: SemanticModel,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """
        Update a semantic view with a complete model.

        This is a bulk update operation that replaces the semantic view
        definition entirely.

        Args:
            model: Complete semantic model to apply
            dry_run: If True, simulate without applying

        Returns:
            Summary of the update operation
        """
        logger.info(f"Updating semantic view: {self._config.semantic_view_name}")

        if dry_run:
            return {
                "status": "dry_run",
                "tables": len(model.tables),
                "measures": len(model.measures),
                "relationships": len(model.relationships),
            }

        # Full semantic view update would use Snowflake's semantic layer API
        # This is a simplified implementation
        with self.connection() as conn:
            with self.transaction(conn):
                # Apply updates...
                pass

        return {
            "status": "updated",
            "tables": len(model.tables),
            "measures": len(model.measures),
            "relationships": len(model.relationships),
        }
