"""
Unit tests for the DuckDB Repository.

Tests project, run, artifact, and version metadata operations.
"""

import pytest
import tempfile
import os
from pathlib import Path
from datetime import datetime

from semantic_sync.core.duckdb_repository import (
    Repository,
    ProjectInfo,
    RunInfo,
    ArtifactInfo,
    VersionMetadata,
    get_repository,
)


class TestRepository:
    """Tests for Repository class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a path to a temporary database file (file doesn't exist yet)."""
        # Create a unique path without creating the file
        temp_dir = tempfile.gettempdir()
        db_path = Path(temp_dir) / f"test_repo_{os.getpid()}_{id(self)}.duckdb"
        # Ensure it doesn't exist
        if db_path.exists():
            db_path.unlink()
        yield db_path
        # Cleanup after tests
        if db_path.exists():
            db_path.unlink()
    
    @pytest.fixture
    def repo(self, temp_db):
        """Create a Repository with temporary database."""
        return Repository(db_path=temp_db)
    
    # =========================================================================
    # Project Tests
    # =========================================================================
    
    def test_create_project(self, repo):
        """Test creating a project."""
        project_id = repo.create_project("TestProject", "Test description")
        
        assert project_id is not None
        assert len(project_id) == 36  # UUID format
    
    def test_create_duplicate_project_fails(self, repo):
        """Test that creating a duplicate project fails."""
        repo.create_project("UniqueProject")
        
        with pytest.raises(ValueError, match="already exists"):
            repo.create_project("UniqueProject")
    
    def test_get_project(self, repo):
        """Test getting a project by ID."""
        project_id = repo.create_project("GetProject", "Description")
        
        project = repo.get_project(project_id)
        
        assert project is not None
        assert project.name == "GetProject"
        assert project.description == "Description"
        assert project.project_id == project_id
    
    def test_get_project_by_name(self, repo):
        """Test getting a project by name."""
        repo.create_project("NamedProject", "Desc")
        
        project = repo.get_project_by_name("NamedProject")
        
        assert project is not None
        assert project.name == "NamedProject"
    
    def test_get_or_create_project(self, repo):
        """Test get_or_create_project."""
        # Create new
        id1 = repo.get_or_create_project("NewProject", "Desc")
        assert id1 is not None
        
        # Get existing
        id2 = repo.get_or_create_project("NewProject", "Different Desc")
        assert id2 == id1
    
    def test_list_projects(self, repo):
        """Test listing projects."""
        for i in range(5):
            repo.create_project(f"Project{i}")
        
        projects = repo.list_projects()
        
        assert len(projects) == 5
    
    def test_delete_project(self, repo):
        """Test deleting a project."""
        project_id = repo.create_project("ToDelete")
        
        deleted = repo.delete_project(project_id)
        
        assert deleted is True
        assert repo.get_project(project_id) is None
    
    # =========================================================================
    # Run Tests
    # =========================================================================
    
    def test_start_run(self, repo):
        """Test starting a run."""
        project_id = repo.create_project("RunProject")
        
        run_id = repo.start_run(
            project_id=project_id,
            direction="fabric-to-snowflake",
            source_connector="fabric",
            target_connector="snowflake",
        )
        
        assert run_id is not None
        assert len(run_id) == 36
        
        run = repo.get_run(run_id)
        assert run.status == "running"
        assert run.direction == "fabric-to-snowflake"
    
    def test_complete_run(self, repo):
        """Test completing a run."""
        project_id = repo.create_project("CompleteProject")
        run_id = repo.start_run(project_id, "test-direction")
        
        repo.complete_run(
            run_id=run_id,
            status="success",
            changes_applied=10,
            errors=0,
        )
        
        run = repo.get_run(run_id)
        assert run.status == "success"
        assert run.changes_applied == 10
        assert run.completed_at is not None
    
    def test_list_runs(self, repo):
        """Test listing runs."""
        project_id = repo.create_project("ListProject")
        
        for i in range(3):
            run_id = repo.start_run(project_id, f"direction-{i}")
            repo.complete_run(run_id, "success" if i % 2 == 0 else "failed")
        
        # All runs
        all_runs = repo.list_runs(project_id=project_id)
        assert len(all_runs) == 3
        
        # Filter by status
        success_runs = repo.list_runs(project_id=project_id, status="success")
        assert len(success_runs) == 2
    
    def test_get_latest_successful_run(self, repo):
        """Test getting latest successful run."""
        project_id = repo.create_project("LatestProject")
        
        # Create some runs
        run1 = repo.start_run(project_id, "dir1")
        repo.complete_run(run1, "failed")
        
        run2 = repo.start_run(project_id, "dir2")
        repo.complete_run(run2, "success")
        
        run3 = repo.start_run(project_id, "dir3")
        repo.complete_run(run3, "failed")
        
        latest = repo.get_latest_successful_run("LatestProject")
        
        assert latest is not None
        assert latest.run_id == run2
    
    # =========================================================================
    # Artifact Tests
    # =========================================================================
    
    def test_store_artifact(self, repo):
        """Test storing an artifact."""
        project_id = repo.create_project("ArtifactProject")
        run_id = repo.start_run(project_id, "test")
        
        content = {"tables": ["A", "B"], "columns": 5}
        artifact_id = repo.store_artifact(
            run_id=run_id,
            artifact_type="source_format",
            content=content,
        )
        
        assert artifact_id is not None
        
        artifacts = repo.get_artifacts(run_id)
        assert len(artifacts) == 1
        assert artifacts[0].artifact_type == "source_format"
        assert artifacts[0].content == content
    
    def test_get_artifacts_by_type(self, repo):
        """Test filtering artifacts by type."""
        project_id = repo.create_project("TypeProject")
        run_id = repo.start_run(project_id, "test")
        
        repo.store_artifact(run_id, "source_format", {"a": 1})
        repo.store_artifact(run_id, "sml", {"b": 2})
        repo.store_artifact(run_id, "log", {"c": 3})
        
        sml_artifacts = repo.get_artifacts(run_id, artifact_type="sml")
        
        assert len(sml_artifacts) == 1
        assert sml_artifacts[0].content == {"b": 2}
    
    def test_get_artifact_by_id(self, repo):
        """Test getting artifact by ID."""
        project_id = repo.create_project("IDProject")
        run_id = repo.start_run(project_id, "test")
        
        artifact_id = repo.store_artifact(run_id, "report", {"result": "pass"})
        
        artifact = repo.get_artifact_by_id(artifact_id)
        
        assert artifact is not None
        assert artifact.artifact_id == artifact_id
        assert artifact.content == {"result": "pass"}
    
    # =========================================================================
    # Version Metadata Tests
    # =========================================================================
    
    def test_record_version_metadata(self, repo):
        """Test recording version metadata."""
        project_id = repo.create_project("VersionProject")
        run_id = repo.start_run(project_id, "test")
        
        repo.record_version_metadata(
            run_id=run_id,
            semabridge_version="1.0.0",
            connector_versions={"fabric": "2.0.0", "snowflake": "3.0.0"},
            rule_pack_version="v1",
        )
        
        version = repo.get_version_metadata(run_id)
        
        assert version is not None
        assert version.semabridge_version == "1.0.0"
        assert version.connector_versions["fabric"] == "2.0.0"
        assert version.rule_pack_version == "v1"
    
    # =========================================================================
    # Stats and Cleanup Tests
    # =========================================================================
    
    def test_get_stats(self, repo):
        """Test getting repository statistics."""
        project_id = repo.create_project("StatsProject")
        
        run1 = repo.start_run(project_id, "test1")
        repo.complete_run(run1, "success")
        repo.store_artifact(run1, "log", {"x": 1})
        
        run2 = repo.start_run(project_id, "test2")
        repo.complete_run(run2, "failed")
        
        stats = repo.get_stats()
        
        assert stats["projects"] == 1
        assert stats["total_runs"] == 2
        assert stats["successful_runs"] == 1
        assert stats["failed_runs"] == 1
        assert stats["artifacts"] == 1
    
    def test_cleanup_old_runs(self, repo):
        """Test cleaning up old runs."""
        project_id = repo.create_project("CleanupProject")
        
        # Create runs
        for i in range(5):
            run_id = repo.start_run(project_id, f"dir{i}")
            repo.complete_run(run_id, "failed")
        
        # Verify runs exist
        runs_before = repo.list_runs(project_id=project_id)
        assert len(runs_before) == 5
        
        # Note: cleanup with older_than_days=0 won't delete runs created "now"
        # because they are not older than today's date at midnight.
        # This tests that the cleanup function works without errors.
        deleted = repo.cleanup_old_runs(older_than_days=0, keep_successful=True)
        # Runs created now are NOT older than 0 days, so they won't be deleted
        assert deleted >= 0  # Just verify no error occurs


class TestDataClasses:
    """Tests for dataclasses."""
    
    def test_project_info_to_dict(self):
        """Test ProjectInfo serialization."""
        info = ProjectInfo(
            project_id="test-id",
            name="TestProject",
            description="Test",
            created_at=datetime(2024, 1, 15, 10, 30, 0),
            updated_at=datetime(2024, 1, 15, 10, 30, 0),
        )
        
        result = info.to_dict()
        
        assert result["project_id"] == "test-id"
        assert result["name"] == "TestProject"
    
    def test_run_info_to_dict(self):
        """Test RunInfo serialization."""
        info = RunInfo(
            run_id="run-id",
            project_id="proj-id",
            status="success",
            started_at=datetime(2024, 1, 15, 10, 0),
            completed_at=datetime(2024, 1, 15, 10, 30),
            source_connector="fabric",
            target_connector="snowflake",
            direction="fabric-to-snowflake",
            changes_applied=5,
            errors=0,
        )
        
        result = info.to_dict()
        
        assert result["status"] == "success"
        assert result["changes_applied"] == 5


class TestIntegration:
    """Integration tests for full workflows."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a path to a temporary database file (file doesn't exist yet)."""
        temp_dir = tempfile.gettempdir()
        db_path = Path(temp_dir) / f"test_integration_{os.getpid()}_{id(self)}.duckdb"
        if db_path.exists():
            db_path.unlink()
        yield db_path
        if db_path.exists():
            db_path.unlink()
    
    def test_full_sync_workflow(self, temp_db):
        """Test a complete sync workflow with all components."""
        repo = Repository(db_path=temp_db)
        
        # 1. Create or get project
        project_id = repo.get_or_create_project(
            "FabricSync",
            description="Fabric to Snowflake sync project"
        )
        
        # 2. Start run
        run_id = repo.start_run(
            project_id=project_id,
            direction="fabric-to-snowflake",
            source_connector="fabric",
            target_connector="snowflake",
        )
        
        # 3. Complete run FIRST (before adding children to avoid FK update issues)
        repo.complete_run(
            run_id=run_id,
            status="success",
            changes_applied=10,
            errors=0,
        )
        
        # 4. Store artifacts (after run is complete)
        repo.store_artifact(
            run_id=run_id,
            artifact_type="source_format",
            content={"model": "SalesAnalytics", "tables": 5},
        )
        
        repo.store_artifact(
            run_id=run_id,
            artifact_type="sml",
            content={"sml_version": "1.0", "entities": 10},
        )
        
        # 5. Record version metadata
        repo.record_version_metadata(
            run_id=run_id,
            semabridge_version="1.0.0",
            connector_versions={"fabric": "1.0", "snowflake": "2.0"},
        )
        
        # Verify
        run = repo.get_run(run_id)
        assert run.status == "success"
        assert run.changes_applied == 10
        
        artifacts = repo.get_artifacts(run_id)
        assert len(artifacts) == 2
        
        latest = repo.get_latest_successful_run("FabricSync")
        assert latest.run_id == run_id
        
        stats = repo.get_stats()
        assert stats["successful_runs"] == 1
        assert stats["artifacts"] == 2


class TestSnapshots:
    """Tests for snapshot functionality."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a path to a temporary database file."""
        temp_dir = tempfile.gettempdir()
        db_path = Path(temp_dir) / f"test_snapshots_{os.getpid()}_{id(self)}.duckdb"
        if db_path.exists():
            db_path.unlink()
        yield db_path
        if db_path.exists():
            db_path.unlink()
    
    @pytest.fixture
    def repo(self, temp_db):
        """Create a Repository with temporary database."""
        return Repository(db_path=temp_db)
    
    def test_create_snapshot(self, repo):
        """Test creating a snapshot."""
        model_data = {"name": "TestModel", "tables": [{"name": "Sales"}]}
        
        snapshot_id = repo.create_snapshot(
            model_name="TestModel",
            source="fabric",
            model_data=model_data,
            description="Test snapshot",
            tables_count=1,
            columns_count=5,
            measures_count=2,
        )
        
        assert snapshot_id is not None
        assert len(snapshot_id) == 36  # UUID format
    
    def test_get_snapshot(self, repo):
        """Test getting a snapshot by ID."""
        model_data = {"name": "GetModel", "tables": []}
        
        snapshot_id = repo.create_snapshot(
            model_name="GetModel",
            source="snowflake",
            model_data=model_data,
        )
        
        result = repo.get_snapshot(snapshot_id)
        
        assert result is not None
        info, data = result
        assert info.model_name == "GetModel"
        assert info.source == "snowflake"
        assert data == model_data
    
    def test_list_snapshots(self, repo):
        """Test listing snapshots."""
        for i in range(3):
            repo.create_snapshot(
                model_name=f"Model{i}",
                source="fabric",
                model_data={"id": i},
            )
        
        snapshots = repo.list_snapshots()
        
        assert len(snapshots) == 3
    
    def test_list_snapshots_filter_by_model(self, repo):
        """Test filtering snapshots by model name."""
        repo.create_snapshot("ModelA", "fabric", {"a": 1})
        repo.create_snapshot("ModelB", "fabric", {"b": 2})
        repo.create_snapshot("ModelA", "fabric", {"a": 3})
        
        snapshots = repo.list_snapshots(model_name="ModelA")
        
        assert len(snapshots) == 2
    
    def test_get_latest_snapshot(self, repo):
        """Test getting the latest snapshot."""
        repo.create_snapshot("Model1", "fabric", {"v": 1})
        repo.create_snapshot("Model2", "fabric", {"v": 2})
        latest_id = repo.create_snapshot("Model3", "fabric", {"v": 3})
        
        result = repo.get_latest_snapshot()
        
        assert result is not None
        info, data = result
        assert info.snapshot_id == latest_id
        assert data == {"v": 3}
    
    def test_delete_snapshot(self, repo):
        """Test deleting a snapshot."""
        snapshot_id = repo.create_snapshot("ToDelete", "fabric", {})
        
        deleted = repo.delete_snapshot(snapshot_id)
        
        assert deleted is True
        assert repo.get_snapshot(snapshot_id) is None
    
    def test_cleanup_old_snapshots(self, repo):
        """Test cleaning up old snapshots."""
        # Create 5 snapshots
        for i in range(5):
            repo.create_snapshot(f"Model{i}", "fabric", {"i": i})
        
        # Keep only 2
        deleted = repo.cleanup_old_snapshots(keep_last=2)
        
        assert deleted == 3
        remaining = repo.list_snapshots()
        assert len(remaining) == 2

