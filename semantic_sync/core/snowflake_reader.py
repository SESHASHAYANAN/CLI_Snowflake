"""
Snowflake Semantic View reader.

Extracts semantic metadata from Snowflake Semantic Views for synchronization.
"""

from contextlib import contextmanager
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
)
from semantic_sync.utils.exceptions import ConnectionError, ResourceNotFoundError
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)


class SnowflakeReader:
    """
    Reads semantic metadata from Snowflake Semantic Views.

    Extracts tables, columns, measures, and relationships from Snowflake's
    semantic layer for synchronization with other platforms.
    """

    def __init__(self, config: SnowflakeConfig) -> None:
        """
        Initialize the Snowflake reader.

        Args:
            config: Snowflake connection configuration
        """
        self._config = config
        self._connection: SnowflakeConnection | None = None

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
            logger.debug("Connecting to Snowflake", account=self._config.account)
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
                logger.debug("Snowflake connection closed")

    def test_connection(self) -> bool:
        """
        Test Snowflake connectivity.

        Returns:
            True if connection succeeds

        Raises:
            ConnectionError: If connection fails
        """
        with self.connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute("SELECT CURRENT_VERSION()")
                version = cursor.fetchone()
                logger.info("Snowflake connection successful", version=version[0] if version else "unknown")
                return True
            finally:
                cursor.close()

    def read_semantic_view(self) -> SemanticModel:
        """
        Read semantic metadata from the configured semantic view.

        Returns:
            SemanticModel containing all metadata

        Raises:
            ResourceNotFoundError: If semantic view doesn't exist
            ConnectionError: If database operation fails
        """
        view_name = self._config.semantic_view_name
        logger.info(f"Reading semantic view: {view_name}")

        with self.connection() as conn:
            # Verify view exists
            if not self._view_exists(conn, view_name):
                raise ResourceNotFoundError(
                    f"Semantic view not found: {view_name}",
                    resource_type="semantic_view",
                    resource_id=view_name,
                )

            # Extract metadata
            tables = self._read_tables(conn, view_name)
            measures = self._read_measures(conn, view_name)
            relationships = self._read_relationships(conn, view_name)

            model = SemanticModel(
                name=view_name,
                source="snowflake",
                tables=tables,
                measures=measures,
                relationships=relationships,
                metadata={
                    "database": self._config.database,
                    "schema": self._config.schema_name,
                },
            )

            logger.info(
                f"Read semantic model: {len(tables)} tables, "
                f"{len(measures)} measures, {len(relationships)} relationships"
            )
            return model

    def _view_exists(self, conn: SnowflakeConnection, view_name: str) -> bool:
        """Check if a semantic view exists."""
        cursor = conn.cursor()
        try:
            query = """
                SELECT COUNT(*)
                FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_NAME = %s
                AND TABLE_SCHEMA = %s
            """
            cursor.execute(query, (view_name, self._config.schema_name))
            result = cursor.fetchone()
            return result[0] > 0 if result else False
        finally:
            cursor.close()

    def _read_tables(
        self,
        conn: SnowflakeConnection,
        view_name: str,
    ) -> list[SemanticTable]:
        """Extract table definitions from semantic view."""
        cursor = conn.cursor()
        tables: list[SemanticTable] = []

        try:
            # Query semantic layer metadata tables
            # This is a simplified example - real implementation would
            # query Snowflake's semantic layer metadata
            query = """
                SELECT
                    TABLE_NAME,
                    COMMENT
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s
                AND TABLE_TYPE = 'BASE TABLE'
            """
            cursor.execute(query, (self._config.schema_name,))

            for row in cursor.fetchall():
                table_name = row[0]
                description = row[1] or ""

                # Get columns for this table
                columns = self._read_columns(conn, table_name)

                tables.append(
                    SemanticTable(
                        name=table_name,
                        description=description,
                        columns=columns,
                        source_table=f"{self._config.database}.{self._config.schema_name}.{table_name}",
                    )
                )

        except ProgrammingError as e:
            logger.warning(f"Error reading tables: {e}")
        finally:
            cursor.close()

        return tables

    def _read_columns(
        self,
        conn: SnowflakeConnection,
        table_name: str,
    ) -> list[SemanticColumn]:
        """Extract column definitions for a table."""
        cursor = conn.cursor()
        columns: list[SemanticColumn] = []

        try:
            query = """
                SELECT
                    COLUMN_NAME,
                    DATA_TYPE,
                    IS_NULLABLE,
                    COMMENT
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """
            cursor.execute(query, (self._config.schema_name, table_name))

            for row in cursor.fetchall():
                columns.append(
                    SemanticColumn(
                        name=row[0],
                        data_type=row[1],
                        is_nullable=row[2] == "YES",
                        description=row[3] or "",
                    )
                )

        except ProgrammingError as e:
            logger.warning(f"Error reading columns for {table_name}: {e}")
        finally:
            cursor.close()

        return columns

    def _read_measures(
        self,
        conn: SnowflakeConnection,
        view_name: str,
    ) -> list[SemanticMeasure]:
        """
        Extract measure definitions from semantic view.

        Note: This is a placeholder for actual semantic layer measure extraction.
        Real implementation would query Snowflake's semantic layer metadata.
        """
        # Placeholder - would need to query actual semantic layer metadata
        # Snowflake's semantic layer stores measures in specific metadata tables
        return []

    def _read_relationships(
        self,
        conn: SnowflakeConnection,
        view_name: str,
    ) -> list[SemanticRelationship]:
        """
        Extract relationship definitions from semantic view.

        Note: This is a placeholder for actual semantic layer relationship extraction.
        """
        # Placeholder - would need to query actual semantic layer metadata
        return []

    def get_raw_metadata(self) -> dict[str, Any]:
        """
        Get raw metadata dictionary for debugging or advanced use cases.

        Returns:
            Dict containing raw semantic view metadata
        """
        model = self.read_semantic_view()
        return model.model_dump()
