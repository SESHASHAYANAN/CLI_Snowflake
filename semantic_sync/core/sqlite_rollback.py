"""
SQLite-based Rollback Manager for Semantic Model Snapshots.

Provides version control and rollback capability for semantic models
using a local SQLite database.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
)
from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)

# Default database location
DEFAULT_DB_DIR = Path.home() / ".semabridge"
DEFAULT_DB_PATH = DEFAULT_DB_DIR / "rollback.db"


@dataclass
class SnapshotInfo:
    """Information about a stored snapshot."""
    
    snapshot_id: str
    model_name: str
    source: str
    created_at: datetime
    tables_count: int
    columns_count: int
    measures_count: int
    description: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "snapshot_id": self.snapshot_id,
            "model_name": self.model_name,
            "source": self.source,
            "created_at": self.created_at.isoformat(),
            "tables_count": self.tables_count,
            "columns_count": self.columns_count,
            "measures_count": self.measures_count,
            "description": self.description,
        }


class RollbackManager:
    """
    Manages semantic model snapshots for rollback capability.
    
    Uses SQLite for local storage of model versions, enabling:
    - Pre-sync snapshot creation
    - Rollback to previous versions
    - Audit trail of sync operations
    """
    
    def __init__(self, db_path: Path | str | None = None):
        """
        Initialize the rollback manager.
        
        Args:
            db_path: Path to SQLite database. Uses default if not specified.
        """
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        
        self.db_path = Path(db_path)
        self._ensure_db_exists()
        
    def _ensure_db_exists(self) -> None:
        """Create database and tables if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Snapshots table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id TEXT PRIMARY KEY,
                    model_name TEXT NOT NULL,
                    source TEXT NOT NULL,
                    model_json TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description TEXT,
                    tables_count INTEGER DEFAULT 0,
                    columns_count INTEGER DEFAULT 0,
                    measures_count INTEGER DEFAULT 0
                )
            """)
            
            # Sync history table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_history (
                    sync_id TEXT PRIMARY KEY,
                    snapshot_id TEXT,
                    direction TEXT NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP,
                    completed_at TIMESTAMP,
                    changes_applied INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    error_message TEXT,
                    FOREIGN KEY (snapshot_id) REFERENCES snapshots(snapshot_id)
                )
            """)
            
            conn.commit()
            logger.debug(f"SQLite database initialized at {self.db_path}")
    
    def create_snapshot(
        self,
        model: SemanticModel,
        description: str | None = None,
    ) -> str:
        """
        Create a snapshot of a semantic model.
        
        Args:
            model: Semantic model to snapshot
            description: Optional description for the snapshot
            
        Returns:
            Snapshot ID
        """
        snapshot_id = str(uuid.uuid4())
        
        # Serialize model to JSON
        model_dict = self._model_to_dict(model)
        model_json = json.dumps(model_dict, default=str)
        
        # Count entities
        tables_count = len(model.tables)
        columns_count = sum(len(t.columns) for t in model.tables)
        measures_count = len(model.measures)
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO snapshots 
                (snapshot_id, model_name, source, model_json, description,
                 tables_count, columns_count, measures_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                snapshot_id,
                model.name,
                model.source,
                model_json,
                description,
                tables_count,
                columns_count,
                measures_count,
            ))
            conn.commit()
        
        logger.info(f"Created snapshot {snapshot_id} for model '{model.name}'")
        return snapshot_id
    
    def restore_snapshot(self, snapshot_id: str) -> SemanticModel:
        """
        Restore a semantic model from a snapshot.
        
        Args:
            snapshot_id: ID of the snapshot to restore
            
        Returns:
            Restored SemanticModel
            
        Raises:
            ValueError: If snapshot not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT model_json FROM snapshots WHERE snapshot_id = ?",
                (snapshot_id,)
            )
            row = cursor.fetchone()
            
        if not row:
            raise ValueError(f"Snapshot not found: {snapshot_id}")
        
        model_dict = json.loads(row[0])
        model = self._dict_to_model(model_dict)
        
        logger.info(f"Restored model '{model.name}' from snapshot {snapshot_id}")
        return model
    
    def get_latest_snapshot(self, model_name: str | None = None) -> SnapshotInfo | None:
        """
        Get the most recent snapshot.
        
        Args:
            model_name: Optional filter by model name
            
        Returns:
            SnapshotInfo or None if no snapshots exist
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if model_name:
                cursor.execute("""
                    SELECT * FROM snapshots 
                    WHERE model_name = ?
                    ORDER BY created_at DESC LIMIT 1
                """, (model_name,))
            else:
                cursor.execute("""
                    SELECT * FROM snapshots 
                    ORDER BY created_at DESC LIMIT 1
                """)
            
            row = cursor.fetchone()
            
        if not row:
            return None
        
        return SnapshotInfo(
            snapshot_id=row["snapshot_id"],
            model_name=row["model_name"],
            source=row["source"],
            created_at=datetime.fromisoformat(row["created_at"]),
            tables_count=row["tables_count"],
            columns_count=row["columns_count"],
            measures_count=row["measures_count"],
            description=row["description"],
        )
    
    def list_snapshots(
        self,
        limit: int = 20,
        model_name: str | None = None,
    ) -> list[SnapshotInfo]:
        """
        List available snapshots.
        
        Args:
            limit: Maximum number of snapshots to return
            model_name: Optional filter by model name
            
        Returns:
            List of SnapshotInfo objects
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            if model_name:
                cursor.execute("""
                    SELECT * FROM snapshots 
                    WHERE model_name = ?
                    ORDER BY created_at DESC LIMIT ?
                """, (model_name, limit))
            else:
                cursor.execute("""
                    SELECT * FROM snapshots 
                    ORDER BY created_at DESC LIMIT ?
                """, (limit,))
            
            rows = cursor.fetchall()
        
        return [
            SnapshotInfo(
                snapshot_id=row["snapshot_id"],
                model_name=row["model_name"],
                source=row["source"],
                created_at=datetime.fromisoformat(row["created_at"]),
                tables_count=row["tables_count"],
                columns_count=row["columns_count"],
                measures_count=row["measures_count"],
                description=row["description"],
            )
            for row in rows
        ]
    
    def delete_snapshot(self, snapshot_id: str) -> bool:
        """
        Delete a specific snapshot.
        
        Args:
            snapshot_id: ID of the snapshot to delete
            
        Returns:
            True if deleted, False if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM snapshots WHERE snapshot_id = ?",
                (snapshot_id,)
            )
            deleted = cursor.rowcount > 0
            conn.commit()
        
        if deleted:
            logger.info(f"Deleted snapshot {snapshot_id}")
        return deleted
    
    def cleanup_old_snapshots(self, keep_last: int = 10) -> int:
        """
        Remove old snapshots, keeping only the most recent ones.
        
        Args:
            keep_last: Number of recent snapshots to keep
            
        Returns:
            Number of snapshots deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Get IDs of snapshots to keep
            cursor.execute("""
                SELECT snapshot_id FROM snapshots
                ORDER BY created_at DESC LIMIT ?
            """, (keep_last,))
            keep_ids = [row[0] for row in cursor.fetchall()]
            
            if keep_ids:
                placeholders = ",".join("?" * len(keep_ids))
                cursor.execute(f"""
                    DELETE FROM snapshots 
                    WHERE snapshot_id NOT IN ({placeholders})
                """, keep_ids)
            else:
                cursor.execute("DELETE FROM snapshots")
            
            deleted = cursor.rowcount
            conn.commit()
        
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old snapshots")
        return deleted
    
    def record_sync(
        self,
        snapshot_id: str | None,
        direction: str,
        status: str,
        started_at: datetime,
        completed_at: datetime | None = None,
        changes_applied: int = 0,
        errors: int = 0,
        error_message: str | None = None,
    ) -> str:
        """
        Record a sync operation in history.
        
        Args:
            snapshot_id: Associated pre-sync snapshot (if any)
            direction: Sync direction (e.g., "fabric-to-snowflake")
            status: Status ("success", "failed", "partial")
            started_at: When sync started
            completed_at: When sync completed
            changes_applied: Number of changes applied
            errors: Number of errors
            error_message: Error message if failed
            
        Returns:
            Sync ID
        """
        sync_id = str(uuid.uuid4())
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO sync_history
                (sync_id, snapshot_id, direction, status, started_at,
                 completed_at, changes_applied, errors, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sync_id,
                snapshot_id,
                direction,
                status,
                started_at.isoformat(),
                completed_at.isoformat() if completed_at else None,
                changes_applied,
                errors,
                error_message,
            ))
            conn.commit()
        
        return sync_id
    
    def _model_to_dict(self, model: SemanticModel) -> dict[str, Any]:
        """Serialize a SemanticModel to dictionary."""
        return {
            "name": model.name,
            "source": model.source,
            "description": model.description,
            "tables": [
                {
                    "name": t.name,
                    "description": t.description,
                    "source_table": t.source_table,
                    "is_hidden": t.is_hidden,
                    "columns": [
                        {
                            "name": c.name,
                            "data_type": c.data_type,
                            "description": c.description,
                            "is_nullable": c.is_nullable,
                            "is_hidden": c.is_hidden,
                            "format_string": c.format_string,
                        }
                        for c in t.columns
                    ],
                }
                for t in model.tables
            ],
            "measures": [
                {
                    "name": m.name,
                    "expression": m.expression,
                    "description": m.description,
                    "format_string": m.format_string,
                    "table_name": m.table_name,
                }
                for m in model.measures
            ],
            "relationships": [
                {
                    "name": r.name,
                    "from_table": r.from_table,
                    "from_column": r.from_column,
                    "to_table": r.to_table,
                    "to_column": r.to_column,
                    "cardinality": r.cardinality,
                    "cross_filter_direction": r.cross_filter_direction,
                }
                for r in model.relationships
            ],
        }
    
    def _dict_to_model(self, data: dict[str, Any]) -> SemanticModel:
        """Deserialize a dictionary to SemanticModel."""
        tables = [
            SemanticTable(
                name=t["name"],
                description=t.get("description"),
                source_table=t.get("source_table"),
                is_hidden=t.get("is_hidden", False),
                columns=[
                    SemanticColumn(
                        name=c["name"],
                        data_type=c["data_type"],
                        description=c.get("description"),
                        is_nullable=c.get("is_nullable", True),
                        is_hidden=c.get("is_hidden", False),
                        format_string=c.get("format_string"),
                    )
                    for c in t.get("columns", [])
                ],
            )
            for t in data.get("tables", [])
        ]
        
        measures = [
            SemanticMeasure(
                name=m["name"],
                expression=m["expression"],
                description=m.get("description"),
                format_string=m.get("format_string"),
                table_name=m.get("table_name"),
            )
            for m in data.get("measures", [])
        ]
        
        relationships = [
            SemanticRelationship(
                name=r.get("name"),
                from_table=r["from_table"],
                from_column=r["from_column"],
                to_table=r["to_table"],
                to_column=r["to_column"],
                cardinality=r.get("cardinality", "many-to-one"),
                cross_filter_direction=r.get("cross_filter_direction"),
            )
            for r in data.get("relationships", [])
        ]
        
        return SemanticModel(
            name=data["name"],
            source=data.get("source", "snapshot"),
            description=data.get("description"),
            tables=tables,
            measures=measures,
            relationships=relationships,
        )


def get_rollback_manager(db_path: Path | str | None = None) -> RollbackManager:
    """
    Get a RollbackManager instance.
    
    Args:
        db_path: Optional custom database path
        
    Returns:
        RollbackManager instance
    """
    return RollbackManager(db_path)
