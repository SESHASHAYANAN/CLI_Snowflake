"""
DuckDB-based Embedded Repository for SemaBridge.

Provides persistent storage for project/run metadata, artifacts, and version
information using DuckDB as the embedded database.

Key Features:
- Store project and run identifiers with timestamps and status
- Store artifacts: Source Format, SML, Target Format, logs, conversion reports
- Query runs by date, status, project
- Retrieve artifacts by project_id/run_id
- Get latest successful run for a project
- Never stores secrets or raw credentials
"""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import duckdb

from semantic_sync.utils.logger import get_logger

logger = get_logger(__name__)

# Default database location
DEFAULT_DB_DIR = Path.home() / ".semabridge"
DEFAULT_DB_PATH = DEFAULT_DB_DIR / "repository.duckdb"

# Version of the current semabridge package
SEMABRIDGE_VERSION = "1.0.0"


@dataclass
class ProjectInfo:
    """Information about a project."""
    
    project_id: str
    name: str
    description: str | None
    created_at: datetime
    updated_at: datetime
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "project_id": self.project_id,
            "name": self.name,
            "description": self.description,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


@dataclass
class RunInfo:
    """Information about a sync run."""
    
    run_id: str
    project_id: str
    status: str  # 'running', 'success', 'failed', 'partial'
    started_at: datetime
    completed_at: datetime | None
    source_connector: str | None
    target_connector: str | None
    direction: str | None
    changes_applied: int
    errors: int
    error_message: str | None = None
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": self.run_id,
            "project_id": self.project_id,
            "status": self.status,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "source_connector": self.source_connector,
            "target_connector": self.target_connector,
            "direction": self.direction,
            "changes_applied": self.changes_applied,
            "errors": self.errors,
            "error_message": self.error_message,
        }


@dataclass
class ArtifactInfo:
    """Information about a stored artifact."""
    
    artifact_id: str
    run_id: str
    artifact_type: str  # 'source_format', 'sml', 'target_format', 'log', 'report'
    content: dict[str, Any]
    content_hash: str
    created_at: datetime
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "artifact_id": self.artifact_id,
            "run_id": self.run_id,
            "artifact_type": self.artifact_type,
            "content": self.content,
            "content_hash": self.content_hash,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class VersionMetadata:
    """Version metadata for a run."""
    
    run_id: str
    semabridge_version: str
    connector_versions: dict[str, str]
    rule_pack_version: str | None
    created_at: datetime
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "run_id": self.run_id,
            "semabridge_version": self.semabridge_version,
            "connector_versions": self.connector_versions,
            "rule_pack_version": self.rule_pack_version,
            "created_at": self.created_at.isoformat(),
        }


@dataclass
class SnapshotInfo:
    """Information about a model snapshot."""
    
    snapshot_id: str
    model_name: str
    source: str
    created_at: datetime
    description: str | None
    tables_count: int
    columns_count: int
    measures_count: int
    
    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "snapshot_id": self.snapshot_id,
            "model_name": self.model_name,
            "source": self.source,
            "created_at": self.created_at.isoformat(),
            "description": self.description,
            "tables_count": self.tables_count,
            "columns_count": self.columns_count,
            "measures_count": self.measures_count,
        }


class Repository:
    """
    DuckDB-based embedded repository for SemaBridge.
    
    Provides persistent storage for:
    - Projects: Named containers for runs
    - Runs: Execution records with status, timestamps, connectors
    - Artifacts: Source Format, SML, Target Format, logs, reports
    - Version Metadata: semabridge version, connector versions, rule packs
    
    Security: Never stores secrets or raw credentials.
    """
    
    def __init__(self, db_path: Path | str | None = None):
        """
        Initialize the repository.
        
        Args:
            db_path: Path to DuckDB database. Uses default if not specified.
        """
        if db_path is None:
            db_path = DEFAULT_DB_PATH
        
        self.db_path = Path(db_path)
        self._ensure_db_exists()
    
    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a database connection."""
        return duckdb.connect(str(self.db_path))
    
    def _ensure_db_exists(self) -> None:
        """Create database and tables if they don't exist."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        with self._get_connection() as conn:
            # Projects table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    project_id VARCHAR PRIMARY KEY,
                    name VARCHAR NOT NULL UNIQUE,
                    description VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Runs table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS runs (
                    run_id VARCHAR PRIMARY KEY,
                    project_id VARCHAR NOT NULL,
                    status VARCHAR NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    source_connector VARCHAR,
                    target_connector VARCHAR,
                    direction VARCHAR,
                    changes_applied INTEGER DEFAULT 0,
                    errors INTEGER DEFAULT 0,
                    error_message VARCHAR,
                    FOREIGN KEY (project_id) REFERENCES projects(project_id)
                )
            """)
            
            # Artifacts table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    artifact_type VARCHAR NOT NULL,
                    content JSON,
                    content_hash VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id)
                )
            """)
            
            # Version metadata table - use VARCHAR id since DuckDB doesn't auto-increment well
            conn.execute("""
                CREATE TABLE IF NOT EXISTS version_metadata (
                    id VARCHAR PRIMARY KEY,
                    run_id VARCHAR NOT NULL,
                    semabridge_version VARCHAR NOT NULL,
                    connector_versions JSON,
                    rule_pack_version VARCHAR,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (run_id) REFERENCES runs(run_id)
                )
            """)
            
            # Snapshots table (for model rollback capability)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    snapshot_id VARCHAR PRIMARY KEY,
                    model_name VARCHAR NOT NULL,
                    source VARCHAR NOT NULL,
                    model_json JSON NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    description VARCHAR,
                    tables_count INTEGER DEFAULT 0,
                    columns_count INTEGER DEFAULT 0,
                    measures_count INTEGER DEFAULT 0
                )
            """)
            
            # Create indexes for common queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_project ON runs(project_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_run ON artifacts(run_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_artifacts_type ON artifacts(artifact_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_model ON snapshots(model_name)")
            
            logger.debug(f"DuckDB repository initialized at {self.db_path}")
    
    # =========================================================================
    # Project Operations
    # =========================================================================
    
    def create_project(
        self,
        name: str,
        description: str | None = None,
    ) -> str:
        """
        Create a new project.
        
        Args:
            name: Project name (must be unique)
            description: Optional project description
            
        Returns:
            Project ID
            
        Raises:
            ValueError: If project name already exists
        """
        project_id = str(uuid.uuid4())
        now = datetime.now()
        
        with self._get_connection() as conn:
            try:
                conn.execute("""
                    INSERT INTO projects (project_id, name, description, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?)
                """, [project_id, name, description, now, now])
            except duckdb.ConstraintException:
                raise ValueError(f"Project '{name}' already exists")
        
        logger.info(f"Created project '{name}' with ID {project_id}")
        return project_id
    
    def get_project(self, project_id: str) -> ProjectInfo | None:
        """
        Get a project by ID.
        
        Args:
            project_id: Project ID
            
        Returns:
            ProjectInfo or None if not found
        """
        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT * FROM projects WHERE project_id = ?",
                [project_id]
            ).fetchone()
        
        if not result:
            return None
        
        return ProjectInfo(
            project_id=result[0],
            name=result[1],
            description=result[2],
            created_at=result[3],
            updated_at=result[4],
        )
    
    def get_project_by_name(self, name: str) -> ProjectInfo | None:
        """
        Get a project by name.
        
        Args:
            name: Project name
            
        Returns:
            ProjectInfo or None if not found
        """
        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT * FROM projects WHERE name = ?",
                [name]
            ).fetchone()
        
        if not result:
            return None
        
        return ProjectInfo(
            project_id=result[0],
            name=result[1],
            description=result[2],
            created_at=result[3],
            updated_at=result[4],
        )
    
    def get_or_create_project(self, name: str, description: str | None = None) -> str:
        """
        Get existing project by name or create new one.
        
        Args:
            name: Project name
            description: Description (used only if creating new)
            
        Returns:
            Project ID
        """
        project = self.get_project_by_name(name)
        if project:
            return project.project_id
        return self.create_project(name, description)
    
    def list_projects(self, limit: int = 50) -> list[ProjectInfo]:
        """
        List all projects.
        
        Args:
            limit: Maximum number of projects to return
            
        Returns:
            List of ProjectInfo objects
        """
        with self._get_connection() as conn:
            results = conn.execute(
                "SELECT * FROM projects ORDER BY updated_at DESC LIMIT ?",
                [limit]
            ).fetchall()
        
        return [
            ProjectInfo(
                project_id=row[0],
                name=row[1],
                description=row[2],
                created_at=row[3],
                updated_at=row[4],
            )
            for row in results
        ]
    
    def delete_project(self, project_id: str, cascade: bool = False) -> bool:
        """
        Delete a project.
        
        Args:
            project_id: Project ID to delete
            cascade: If True, also delete all runs and artifacts
            
        Returns:
            True if deleted, False if not found
        """
        # Check if project exists first
        if self.get_project(project_id) is None:
            return False
        
        with self._get_connection() as conn:
            if cascade:
                # Get all runs for this project
                runs = conn.execute(
                    "SELECT run_id FROM runs WHERE project_id = ?",
                    [project_id]
                ).fetchall()
                
                for (run_id,) in runs:
                    conn.execute("DELETE FROM artifacts WHERE run_id = ?", [run_id])
                    conn.execute("DELETE FROM version_metadata WHERE run_id = ?", [run_id])
                
                conn.execute("DELETE FROM runs WHERE project_id = ?", [project_id])
            
            conn.execute(
                "DELETE FROM projects WHERE project_id = ?",
                [project_id]
            )
        
        logger.info(f"Deleted project {project_id}")
        return True
    
    # =========================================================================
    # Run Operations
    # =========================================================================
    
    def start_run(
        self,
        project_id: str,
        direction: str,
        source_connector: str | None = None,
        target_connector: str | None = None,
    ) -> str:
        """
        Start a new run.
        
        Args:
            project_id: Project ID
            direction: Sync direction (e.g., 'fabric-to-snowflake')
            source_connector: Source connector type
            target_connector: Target connector type
            
        Returns:
            Run ID
        """
        run_id = str(uuid.uuid4())
        now = datetime.now()
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO runs 
                (run_id, project_id, status, started_at, source_connector, 
                 target_connector, direction, changes_applied, errors)
                VALUES (?, ?, 'running', ?, ?, ?, ?, 0, 0)
            """, [run_id, project_id, now, source_connector, target_connector, direction])
            
            # Update project's updated_at
            conn.execute(
                "UPDATE projects SET updated_at = ? WHERE project_id = ?",
                [now, project_id]
            )
        
        logger.info(f"Started run {run_id} for project {project_id}")
        return run_id
    
    def complete_run(
        self,
        run_id: str,
        status: str,
        changes_applied: int = 0,
        errors: int = 0,
        error_message: str | None = None,
    ) -> None:
        """
        Complete a run.
        
        Args:
            run_id: Run ID
            status: Final status ('success', 'failed', 'partial')
            changes_applied: Number of changes applied
            errors: Number of errors
            error_message: Error message if failed
        """
        now = datetime.now()
        
        with self._get_connection() as conn:
            conn.execute("""
                UPDATE runs 
                SET status = ?, completed_at = ?, changes_applied = ?, 
                    errors = ?, error_message = ?
                WHERE run_id = ?
            """, [status, now, changes_applied, errors, error_message, run_id])
        
        logger.info(f"Completed run {run_id} with status {status}")
    
    def get_run(self, run_id: str) -> RunInfo | None:
        """
        Get a run by ID.
        
        Args:
            run_id: Run ID
            
        Returns:
            RunInfo or None if not found
        """
        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT * FROM runs WHERE run_id = ?",
                [run_id]
            ).fetchone()
        
        if not result:
            return None
        
        return RunInfo(
            run_id=result[0],
            project_id=result[1],
            status=result[2],
            started_at=result[3],
            completed_at=result[4],
            source_connector=result[5],
            target_connector=result[6],
            direction=result[7],
            changes_applied=result[8],
            errors=result[9],
            error_message=result[10],
        )
    
    def list_runs(
        self,
        project_id: str | None = None,
        project_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 50,
    ) -> list[RunInfo]:
        """
        List runs with optional filters.
        
        Args:
            project_id: Filter by project ID
            project_name: Filter by project name
            status: Filter by status
            start_date: Filter runs started after this date
            end_date: Filter runs started before this date
            limit: Maximum number of runs to return
            
        Returns:
            List of RunInfo objects
        """
        query = "SELECT r.* FROM runs r"
        params = []
        conditions = []
        
        if project_name:
            query += " JOIN projects p ON r.project_id = p.project_id"
            conditions.append("p.name = ?")
            params.append(project_name)
        
        if project_id:
            conditions.append("r.project_id = ?")
            params.append(project_id)
        
        if status:
            conditions.append("r.status = ?")
            params.append(status)
        
        if start_date:
            conditions.append("r.started_at >= ?")
            params.append(start_date)
        
        if end_date:
            conditions.append("r.started_at <= ?")
            params.append(end_date)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += " ORDER BY r.started_at DESC LIMIT ?"
        params.append(limit)
        
        with self._get_connection() as conn:
            results = conn.execute(query, params).fetchall()
        
        return [
            RunInfo(
                run_id=row[0],
                project_id=row[1],
                status=row[2],
                started_at=row[3],
                completed_at=row[4],
                source_connector=row[5],
                target_connector=row[6],
                direction=row[7],
                changes_applied=row[8],
                errors=row[9],
                error_message=row[10],
            )
            for row in results
        ]
    
    def get_latest_successful_run(self, project_name: str) -> RunInfo | None:
        """
        Get the latest successful run for a project.
        
        Args:
            project_name: Project name
            
        Returns:
            RunInfo or None if no successful runs exist
        """
        runs = self.list_runs(project_name=project_name, status="success", limit=1)
        return runs[0] if runs else None
    
    # =========================================================================
    # Artifact Operations
    # =========================================================================
    
    def store_artifact(
        self,
        run_id: str,
        artifact_type: str,
        content: dict[str, Any],
    ) -> str:
        """
        Store an artifact.
        
        Args:
            run_id: Run ID
            artifact_type: Type ('source_format', 'sml', 'target_format', 'log', 'report')
            content: Artifact content as dictionary
            
        Returns:
            Artifact ID
        """
        artifact_id = str(uuid.uuid4())
        content_json = json.dumps(content, default=str)
        content_hash = hashlib.sha256(content_json.encode()).hexdigest()[:16]
        now = datetime.now()
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO artifacts 
                (artifact_id, run_id, artifact_type, content, content_hash, created_at)
                VALUES (?, ?, ?, ?::JSON, ?, ?)
            """, [artifact_id, run_id, artifact_type, content_json, content_hash, now])
        
        logger.debug(f"Stored artifact {artifact_id} of type {artifact_type} for run {run_id}")
        return artifact_id
    
    def get_artifacts(
        self,
        run_id: str,
        artifact_type: str | None = None,
    ) -> list[ArtifactInfo]:
        """
        Get artifacts for a run.
        
        Args:
            run_id: Run ID
            artifact_type: Optional filter by artifact type
            
        Returns:
            List of ArtifactInfo objects
        """
        if artifact_type:
            query = "SELECT * FROM artifacts WHERE run_id = ? AND artifact_type = ?"
            params = [run_id, artifact_type]
        else:
            query = "SELECT * FROM artifacts WHERE run_id = ? ORDER BY created_at"
            params = [run_id]
        
        with self._get_connection() as conn:
            results = conn.execute(query, params).fetchall()
        
        return [
            ArtifactInfo(
                artifact_id=row[0],
                run_id=row[1],
                artifact_type=row[2],
                content=row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {},
                content_hash=row[4],
                created_at=row[5],
            )
            for row in results
        ]
    
    def get_artifact_by_id(self, artifact_id: str) -> ArtifactInfo | None:
        """
        Get an artifact by ID.
        
        Args:
            artifact_id: Artifact ID
            
        Returns:
            ArtifactInfo or None if not found
        """
        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT * FROM artifacts WHERE artifact_id = ?",
                [artifact_id]
            ).fetchone()
        
        if not result:
            return None
        
        return ArtifactInfo(
            artifact_id=result[0],
            run_id=result[1],
            artifact_type=result[2],
            content=result[3] if isinstance(result[3], dict) else json.loads(result[3]) if result[3] else {},
            content_hash=result[4],
            created_at=result[5],
        )
    
    def get_artifacts_by_project(
        self,
        project_id: str,
        artifact_type: str | None = None,
        limit: int = 100,
    ) -> list[ArtifactInfo]:
        """
        Get all artifacts for a project.
        
        Args:
            project_id: Project ID
            artifact_type: Optional filter by artifact type
            limit: Maximum number of artifacts
            
        Returns:
            List of ArtifactInfo objects
        """
        query = """
            SELECT a.* FROM artifacts a
            JOIN runs r ON a.run_id = r.run_id
            WHERE r.project_id = ?
        """
        params = [project_id]
        
        if artifact_type:
            query += " AND a.artifact_type = ?"
            params.append(artifact_type)
        
        query += " ORDER BY a.created_at DESC LIMIT ?"
        params.append(limit)
        
        with self._get_connection() as conn:
            results = conn.execute(query, params).fetchall()
        
        return [
            ArtifactInfo(
                artifact_id=row[0],
                run_id=row[1],
                artifact_type=row[2],
                content=row[3] if isinstance(row[3], dict) else json.loads(row[3]) if row[3] else {},
                content_hash=row[4],
                created_at=row[5],
            )
            for row in results
        ]
    
    # =========================================================================
    # Version Metadata Operations
    # =========================================================================
    
    def record_version_metadata(
        self,
        run_id: str,
        semabridge_version: str | None = None,
        connector_versions: dict[str, str] | None = None,
        rule_pack_version: str | None = None,
    ) -> None:
        """
        Record version metadata for a run.
        
        Args:
            run_id: Run ID
            semabridge_version: SemaBridge version (uses current if not specified)
            connector_versions: Dict of connector name -> version
            rule_pack_version: Rule pack version
        """
        if semabridge_version is None:
            semabridge_version = SEMABRIDGE_VERSION
        
        connector_json = json.dumps(connector_versions or {})
        now = datetime.now()
        
        metadata_id = str(uuid.uuid4())
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO version_metadata 
                (id, run_id, semabridge_version, connector_versions, rule_pack_version, created_at)
                VALUES (?, ?, ?, ?::JSON, ?, ?)
            """, [metadata_id, run_id, semabridge_version, connector_json, rule_pack_version, now])
        
        logger.debug(f"Recorded version metadata for run {run_id}")
    
    def get_version_metadata(self, run_id: str) -> VersionMetadata | None:
        """
        Get version metadata for a run.
        
        Args:
            run_id: Run ID
            
        Returns:
            VersionMetadata or None if not found
        """
        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT * FROM version_metadata WHERE run_id = ?",
                [run_id]
            ).fetchone()
        
        if not result:
            return None
        
        connector_versions = result[3]
        if isinstance(connector_versions, str):
            connector_versions = json.loads(connector_versions)
        elif connector_versions is None:
            connector_versions = {}
        
        return VersionMetadata(
            run_id=result[1],
            semabridge_version=result[2],
            connector_versions=connector_versions,
            rule_pack_version=result[4],
            created_at=result[5],
        )
    
    # =========================================================================
    # Cleanup Operations
    # =========================================================================
    
    def cleanup_old_runs(
        self,
        older_than_days: int = 30,
        keep_successful: bool = True,
    ) -> int:
        """
        Clean up old runs and their artifacts.
        
        Args:
            older_than_days: Delete runs older than this many days
            keep_successful: If True, never delete successful runs
            
        Returns:
            Number of runs deleted
        """
        cutoff = datetime.now().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        # Subtract days manually
        import datetime as dt
        cutoff = cutoff - dt.timedelta(days=older_than_days)
        
        with self._get_connection() as conn:
            # Build query
            if keep_successful:
                query = """
                    SELECT run_id FROM runs 
                    WHERE started_at < ? AND status != 'success'
                """
            else:
                query = "SELECT run_id FROM runs WHERE started_at < ?"
            
            runs = conn.execute(query, [cutoff]).fetchall()
            
            deleted = 0
            for (run_id,) in runs:
                conn.execute("DELETE FROM artifacts WHERE run_id = ?", [run_id])
                conn.execute("DELETE FROM version_metadata WHERE run_id = ?", [run_id])
                conn.execute("DELETE FROM runs WHERE run_id = ?", [run_id])
                deleted += 1
        
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old runs")
        return deleted
    
    def get_stats(self) -> dict[str, Any]:
        """
        Get repository statistics.
        
        Returns:
            Dictionary with counts and sizes
        """
        with self._get_connection() as conn:
            projects = conn.execute("SELECT COUNT(*) FROM projects").fetchone()[0]
            runs = conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
            artifacts = conn.execute("SELECT COUNT(*) FROM artifacts").fetchone()[0]
            successful = conn.execute(
                "SELECT COUNT(*) FROM runs WHERE status = 'success'"
            ).fetchone()[0]
            failed = conn.execute(
                "SELECT COUNT(*) FROM runs WHERE status = 'failed'"
            ).fetchone()[0]
        
        return {
            "projects": projects,
            "total_runs": runs,
            "successful_runs": successful,
            "failed_runs": failed,
            "artifacts": artifacts,
            "database_path": str(self.db_path),
        }
    
    # =========================================================================
    # Snapshot Operations (for model rollback capability)
    # =========================================================================
    
    def create_snapshot(
        self,
        model_name: str,
        source: str,
        model_data: dict[str, Any],
        description: str | None = None,
        tables_count: int = 0,
        columns_count: int = 0,
        measures_count: int = 0,
    ) -> str:
        """
        Create a snapshot of a semantic model.
        
        Args:
            model_name: Name of the model
            source: Source platform (e.g., 'fabric', 'snowflake')
            model_data: Model data as dictionary (JSON-serializable)
            description: Optional snapshot description
            tables_count: Number of tables in model
            columns_count: Number of columns in model
            measures_count: Number of measures in model
            
        Returns:
            Snapshot ID
        """
        snapshot_id = str(uuid.uuid4())
        model_json = json.dumps(model_data, default=str)
        now = datetime.now()
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO snapshots 
                (snapshot_id, model_name, source, model_json, created_at, description,
                 tables_count, columns_count, measures_count)
                VALUES (?, ?, ?, ?::JSON, ?, ?, ?, ?, ?)
            """, [snapshot_id, model_name, source, model_json, now, description,
                  tables_count, columns_count, measures_count])
        
        logger.info(f"Created snapshot {snapshot_id} for model '{model_name}'")
        return snapshot_id
    
    def get_snapshot(self, snapshot_id: str) -> tuple[SnapshotInfo, dict[str, Any]] | None:
        """
        Get a snapshot by ID.
        
        Args:
            snapshot_id: Snapshot ID
            
        Returns:
            Tuple of (SnapshotInfo, model_data) or None if not found
        """
        with self._get_connection() as conn:
            result = conn.execute(
                "SELECT * FROM snapshots WHERE snapshot_id = ?",
                [snapshot_id]
            ).fetchone()
        
        if not result:
            return None
        
        model_data = result[3]
        if isinstance(model_data, str):
            model_data = json.loads(model_data)
        
        info = SnapshotInfo(
            snapshot_id=result[0],
            model_name=result[1],
            source=result[2],
            created_at=result[4],
            description=result[5],
            tables_count=result[6] or 0,
            columns_count=result[7] or 0,
            measures_count=result[8] or 0,
        )
        
        return info, model_data
    
    def list_snapshots(
        self,
        model_name: str | None = None,
        limit: int = 50,
    ) -> list[SnapshotInfo]:
        """
        List snapshots with optional filter.
        
        Args:
            model_name: Optional filter by model name
            limit: Maximum number to return
            
        Returns:
            List of SnapshotInfo objects
        """
        if model_name:
            query = "SELECT * FROM snapshots WHERE model_name = ? ORDER BY created_at DESC LIMIT ?"
            params = [model_name, limit]
        else:
            query = "SELECT * FROM snapshots ORDER BY created_at DESC LIMIT ?"
            params = [limit]
        
        with self._get_connection() as conn:
            results = conn.execute(query, params).fetchall()
        
        return [
            SnapshotInfo(
                snapshot_id=row[0],
                model_name=row[1],
                source=row[2],
                created_at=row[4],
                description=row[5],
                tables_count=row[6] or 0,
                columns_count=row[7] or 0,
                measures_count=row[8] or 0,
            )
            for row in results
        ]
    
    def get_latest_snapshot(self, model_name: str | None = None) -> tuple[SnapshotInfo, dict[str, Any]] | None:
        """
        Get the latest snapshot.
        
        Args:
            model_name: Optional filter by model name
            
        Returns:
            Tuple of (SnapshotInfo, model_data) or None
        """
        snapshots = self.list_snapshots(model_name=model_name, limit=1)
        if not snapshots:
            return None
        return self.get_snapshot(snapshots[0].snapshot_id)
    
    def delete_snapshot(self, snapshot_id: str) -> bool:
        """
        Delete a snapshot.
        
        Args:
            snapshot_id: Snapshot ID
            
        Returns:
            True if deleted, False if not found
        """
        # Check exists first (DuckDB rowcount issue)
        existing = self.get_snapshot(snapshot_id)
        if not existing:
            return False
        
        with self._get_connection() as conn:
            conn.execute("DELETE FROM snapshots WHERE snapshot_id = ?", [snapshot_id])
        
        logger.info(f"Deleted snapshot {snapshot_id}")
        return True
    
    def cleanup_old_snapshots(self, keep_last: int = 5) -> int:
        """
        Remove old snapshots, keeping only the most recent ones.
        
        Args:
            keep_last: Number of most recent snapshots to keep
            
        Returns:
            Number of snapshots deleted
        """
        with self._get_connection() as conn:
            # Get all snapshot IDs ordered by date
            all_snapshots = conn.execute(
                "SELECT snapshot_id FROM snapshots ORDER BY created_at DESC"
            ).fetchall()
            
            # Delete all except the most recent ones
            to_delete = [s[0] for s in all_snapshots[keep_last:]]
            deleted = 0
            for snapshot_id in to_delete:
                conn.execute("DELETE FROM snapshots WHERE snapshot_id = ?", [snapshot_id])
                deleted += 1
        
        if deleted > 0:
            logger.info(f"Cleaned up {deleted} old snapshots")
        return deleted

def get_repository(db_path: Path | str | None = None) -> Repository:
    """
    Get a Repository instance.
    
    Args:
        db_path: Optional custom database path
        
    Returns:
        Repository instance
    """
    return Repository(db_path)
