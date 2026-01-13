"""
Unit tests for the SQLite Rollback Manager.

Tests snapshot creation, restoration, and cleanup functionality.
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime

from semantic_sync.core.sqlite_rollback import RollbackManager, SnapshotInfo
from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
)


class TestRollbackManager:
    """Tests for RollbackManager class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            yield Path(f.name)
        # Cleanup after tests
        Path(f.name).unlink(missing_ok=True)
    
    @pytest.fixture
    def manager(self, temp_db):
        """Create a RollbackManager with temporary database."""
        return RollbackManager(db_path=temp_db)
    
    @pytest.fixture
    def sample_model(self):
        """Create a sample semantic model for testing."""
        return SemanticModel(
            name="TestModel",
            source="fabric",
            description="Test model for snapshot testing",
            tables=[
                SemanticTable(
                    name="Products",
                    description="Product catalog",
                    columns=[
                        SemanticColumn(
                            name="ProductID",
                            data_type="Int64",
                            is_nullable=False,
                        ),
                        SemanticColumn(
                            name="ProductName",
                            data_type="String",
                            description="Name of the product",
                        ),
                        SemanticColumn(
                            name="Price",
                            data_type="Decimal",
                            format_string="$#,##0.00",
                        ),
                    ],
                ),
                SemanticTable(
                    name="Orders",
                    description="Order transactions",
                    columns=[
                        SemanticColumn(name="OrderID", data_type="Int64"),
                        SemanticColumn(name="ProductID", data_type="Int64"),
                        SemanticColumn(name="Quantity", data_type="Int64"),
                    ],
                ),
            ],
            measures=[
                SemanticMeasure(
                    name="Total Revenue",
                    expression="SUM(Orders[Quantity] * Products[Price])",
                    description="Total revenue from all orders",
                ),
            ],
            relationships=[
                SemanticRelationship(
                    name="Orders_Products",
                    from_table="Orders",
                    from_column="ProductID",
                    to_table="Products",
                    to_column="ProductID",
                ),
            ],
        )
    
    def test_create_snapshot(self, manager, sample_model):
        """Test creating a snapshot."""
        snapshot_id = manager.create_snapshot(sample_model, description="Test snapshot")
        
        assert snapshot_id is not None
        assert len(snapshot_id) == 36  # UUID format
        
    def test_restore_snapshot(self, manager, sample_model):
        """Test restoring a model from snapshot."""
        # Create snapshot
        snapshot_id = manager.create_snapshot(sample_model)
        
        # Restore and verify
        restored = manager.restore_snapshot(snapshot_id)
        
        assert restored.name == sample_model.name
        assert restored.source == sample_model.source
        assert len(restored.tables) == len(sample_model.tables)
        assert len(restored.measures) == len(sample_model.measures)
        assert len(restored.relationships) == len(sample_model.relationships)
        
        # Verify table details
        products = next(t for t in restored.tables if t.name == "Products")
        assert products.description == "Product catalog"
        assert len(products.columns) == 3
        
        # Verify column details
        price_col = next(c for c in products.columns if c.name == "Price")
        assert price_col.data_type == "Decimal"
        assert price_col.format_string == "$#,##0.00"
        
    def test_restore_nonexistent_snapshot(self, manager):
        """Test restoring a snapshot that doesn't exist."""
        with pytest.raises(ValueError, match="Snapshot not found"):
            manager.restore_snapshot("nonexistent-id")
            
    def test_list_snapshots(self, manager, sample_model):
        """Test listing snapshots."""
        # Create multiple snapshots
        ids = []
        for i in range(3):
            model = SemanticModel(
                name=f"Model{i}",
                source="fabric",
                tables=[],
            )
            ids.append(manager.create_snapshot(model, description=f"Snapshot {i}"))
        
        # List all
        snapshots = manager.list_snapshots()
        
        assert len(snapshots) == 3
        # Most recent first
        assert snapshots[0].model_name == "Model2"
        assert snapshots[0].description == "Snapshot 2"
        
    def test_list_snapshots_with_limit(self, manager, sample_model):
        """Test listing snapshots with limit."""
        # Create 5 snapshots
        for i in range(5):
            model = SemanticModel(name=f"Model{i}", source="fabric", tables=[])
            manager.create_snapshot(model)
        
        # Limit to 3
        snapshots = manager.list_snapshots(limit=3)
        
        assert len(snapshots) == 3
        
    def test_list_snapshots_filter_by_model(self, manager):
        """Test filtering snapshots by model name."""
        # Create snapshots for different models
        for name in ["ModelA", "ModelA", "ModelB", "ModelA"]:
            model = SemanticModel(name=name, source="fabric", tables=[])
            manager.create_snapshot(model)
        
        # Filter by ModelA
        snapshots = manager.list_snapshots(model_name="ModelA")
        
        assert len(snapshots) == 3
        assert all(s.model_name == "ModelA" for s in snapshots)
        
    def test_get_latest_snapshot(self, manager):
        """Test getting the latest snapshot."""
        # Create snapshots
        for i in range(3):
            model = SemanticModel(name=f"Model{i}", source="fabric", tables=[])
            manager.create_snapshot(model, description=f"Snapshot {i}")
        
        latest = manager.get_latest_snapshot()
        
        assert latest is not None
        assert latest.model_name == "Model2"
        assert latest.description == "Snapshot 2"
        
    def test_get_latest_snapshot_empty(self, manager):
        """Test getting latest when no snapshots exist."""
        latest = manager.get_latest_snapshot()
        assert latest is None
        
    def test_delete_snapshot(self, manager, sample_model):
        """Test deleting a snapshot."""
        snapshot_id = manager.create_snapshot(sample_model)
        
        # Verify it exists
        snapshots = manager.list_snapshots()
        assert len(snapshots) == 1
        
        # Delete
        deleted = manager.delete_snapshot(snapshot_id)
        assert deleted is True
        
        # Verify it's gone
        snapshots = manager.list_snapshots()
        assert len(snapshots) == 0
        
    def test_delete_nonexistent_snapshot(self, manager):
        """Test deleting a snapshot that doesn't exist."""
        deleted = manager.delete_snapshot("nonexistent-id")
        assert deleted is False
        
    def test_cleanup_old_snapshots(self, manager):
        """Test cleaning up old snapshots."""
        # Create 10 snapshots
        for i in range(10):
            model = SemanticModel(name=f"Model{i}", source="fabric", tables=[])
            manager.create_snapshot(model)
        
        # Keep only last 3
        deleted = manager.cleanup_old_snapshots(keep_last=3)
        
        assert deleted == 7
        
        snapshots = manager.list_snapshots()
        assert len(snapshots) == 3
        
    def test_record_sync(self, manager, sample_model):
        """Test recording sync operations."""
        snapshot_id = manager.create_snapshot(sample_model)
        
        sync_id = manager.record_sync(
            snapshot_id=snapshot_id,
            direction="fabric-to-snowflake",
            status="success",
            started_at=datetime.now(),
            completed_at=datetime.now(),
            changes_applied=5,
            errors=0,
        )
        
        assert sync_id is not None
        assert len(sync_id) == 36  # UUID format
        
    def test_snapshot_counts(self, manager, sample_model):
        """Test that snapshot correctly counts entities."""
        snapshot_id = manager.create_snapshot(sample_model)
        
        snapshots = manager.list_snapshots()
        assert len(snapshots) == 1
        
        snapshot = snapshots[0]
        assert snapshot.tables_count == 2
        assert snapshot.columns_count == 6  # 3 + 3
        assert snapshot.measures_count == 1


class TestSnapshotInfo:
    """Tests for SnapshotInfo dataclass."""
    
    def test_to_dict(self):
        """Test serialization to dictionary."""
        info = SnapshotInfo(
            snapshot_id="test-id",
            model_name="TestModel",
            source="fabric",
            created_at=datetime(2024, 1, 15, 10, 30, 0),
            tables_count=2,
            columns_count=5,
            measures_count=1,
            description="Test snapshot",
        )
        
        result = info.to_dict()
        
        assert result["snapshot_id"] == "test-id"
        assert result["model_name"] == "TestModel"
        assert result["source"] == "fabric"
        assert result["created_at"] == "2024-01-15T10:30:00"
        assert result["tables_count"] == 2
        assert result["columns_count"] == 5
        assert result["measures_count"] == 1
        assert result["description"] == "Test snapshot"


class TestRollbackIntegration:
    """Integration tests for rollback workflow."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database file."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            yield Path(f.name)
        Path(f.name).unlink(missing_ok=True)
    
    def test_snapshot_and_restore_preserves_all_data(self, temp_db):
        """Test that snapshot/restore cycle preserves all model data."""
        manager = RollbackManager(db_path=temp_db)
        
        # Create a complex model
        original = SemanticModel(
            name="ComplexModel",
            source="fabric",
            description="A complex model with all features",
            tables=[
                SemanticTable(
                    name="Table1",
                    description="First table",
                    source_table="dbo.Table1",
                    is_hidden=False,
                    columns=[
                        SemanticColumn(
                            name="Col1",
                            data_type="String",
                            description="First column",
                            is_nullable=True,
                            is_hidden=False,
                            format_string="General",
                        ),
                    ],
                ),
            ],
            measures=[
                SemanticMeasure(
                    name="M1",
                    expression="SUM(Table1[Col1])",
                    description="First measure",
                    format_string="$#,##0",
                    table_name="Table1",
                ),
            ],
            relationships=[
                SemanticRelationship(
                    name="R1",
                    from_table="Table1",
                    from_column="Col1",
                    to_table="Table2",
                    to_column="Col2",
                    cardinality="many-to-one",
                    cross_filter_direction="single",
                ),
            ],
        )
        
        # Snapshot and restore
        snapshot_id = manager.create_snapshot(original)
        restored = manager.restore_snapshot(snapshot_id)
        
        # Verify all fields
        assert restored.name == original.name
        assert restored.source == original.source
        assert restored.description == original.description
        
        # Table
        t1 = restored.tables[0]
        assert t1.name == "Table1"
        assert t1.description == "First table"
        assert t1.source_table == "dbo.Table1"
        
        # Column
        c1 = t1.columns[0]
        assert c1.name == "Col1"
        assert c1.data_type == "String"
        assert c1.format_string == "General"
        
        # Measure
        m1 = restored.measures[0]
        assert m1.name == "M1"
        assert m1.expression == "SUM(Table1[Col1])"
        assert m1.table_name == "Table1"
        
        # Relationship
        r1 = restored.relationships[0]
        assert r1.name == "R1"
        assert r1.from_table == "Table1"
        assert r1.cardinality == "many-to-one"
        assert r1.cross_filter_direction == "single"
