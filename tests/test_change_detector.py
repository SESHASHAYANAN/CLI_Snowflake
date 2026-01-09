"""
Unit tests for the change detector module.
"""

import pytest
from datetime import datetime

from semantic_sync.core.change_detector import (
    ChangeDetector,
    ChangeType,
    Change,
    ChangeReport,
)
from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
    DataType,
)


class TestChangeType:
    """Tests for ChangeType enum."""

    def test_change_type_values(self):
        """Test ChangeType enum values."""
        assert ChangeType.ADDED.value == "added"
        assert ChangeType.MODIFIED.value == "modified"
        assert ChangeType.REMOVED.value == "removed"
        assert ChangeType.UNCHANGED.value == "unchanged"


class TestChange:
    """Tests for Change dataclass."""

    def test_change_str_simple(self):
        """Test Change string representation without parent."""
        change = Change(
            change_type=ChangeType.ADDED,
            entity_type="table",
            entity_name="customers",
        )
        assert "ADDED" in str(change)
        assert "table" in str(change)
        assert "customers" in str(change)

    def test_change_str_with_parent(self):
        """Test Change string representation with parent entity."""
        change = Change(
            change_type=ChangeType.MODIFIED,
            entity_type="column",
            entity_name="email",
            parent_entity="customers",
        )
        assert "MODIFIED" in str(change)
        assert "customers.email" in str(change)

    def test_change_to_dict(self):
        """Test Change serialization to dictionary."""
        change = Change(
            change_type=ChangeType.ADDED,
            entity_type="table",
            entity_name="orders",
            new_value={"name": "orders"},
        )
        result = change.to_dict()

        assert result["change_type"] == "added"
        assert result["entity_type"] == "table"
        assert result["entity_name"] == "orders"
        assert result["new_value"] == {"name": "orders"}


class TestChangeReport:
    """Tests for ChangeReport dataclass."""

    def test_empty_report(self):
        """Test report with no changes."""
        report = ChangeReport(source="source", target="target", changes=[])
        
        assert not report.has_changes
        assert report.summary()["total"] == 0

    def test_report_with_changes(self):
        """Test report with various changes."""
        changes = [
            Change(change_type=ChangeType.ADDED, entity_type="table", entity_name="t1"),
            Change(change_type=ChangeType.ADDED, entity_type="table", entity_name="t2"),
            Change(change_type=ChangeType.MODIFIED, entity_type="column", entity_name="c1"),
            Change(change_type=ChangeType.REMOVED, entity_type="table", entity_name="t3"),
        ]
        report = ChangeReport(source="source", target="target", changes=changes)

        assert report.has_changes
        assert len(report.additions) == 2
        assert len(report.modifications) == 1
        assert len(report.removals) == 1
        
        summary = report.summary()
        assert summary["added"] == 2
        assert summary["modified"] == 1
        assert summary["removed"] == 1
        assert summary["total"] == 4


class TestChangeDetector:
    """Tests for ChangeDetector class."""

    @pytest.fixture
    def detector(self):
        """Create a ChangeDetector instance."""
        return ChangeDetector()

    @pytest.fixture
    def source_model(self):
        """Create a source semantic model for testing."""
        return SemanticModel(
            name="source_model",
            source="fabric",
            tables=[
                SemanticTable(
                    name="customers",
                    description="Customer data",
                    columns=[
                        SemanticColumn(
                            name="id",
                            data_type="INTEGER",
                            is_nullable=False,
                        ),
                        SemanticColumn(
                            name="name",
                            data_type="VARCHAR",
                            description="Customer name",
                        ),
                        SemanticColumn(
                            name="email",
                            data_type="VARCHAR",
                            description="Customer email",
                        ),
                    ],
                ),
                SemanticTable(
                    name="orders",
                    description="Order data",
                    columns=[
                        SemanticColumn(name="order_id", data_type="INTEGER"),
                        SemanticColumn(name="customer_id", data_type="INTEGER"),
                    ],
                ),
            ],
            measures=[
                SemanticMeasure(
                    name="total_orders",
                    expression="COUNT(orders[order_id])",
                    description="Total orders count",
                ),
            ],
        )

    @pytest.fixture
    def target_model(self):
        """Create a target semantic model for testing."""
        return SemanticModel(
            name="target_model",
            source="snowflake",
            tables=[
                SemanticTable(
                    name="customers",
                    description="Customer information",  # Different description
                    columns=[
                        SemanticColumn(
                            name="id",
                            data_type="INTEGER",
                            is_nullable=False,
                        ),
                        SemanticColumn(
                            name="name",
                            data_type="VARCHAR",
                            description="Customer name",
                        ),
                        # Missing 'email' column
                    ],
                ),
                # Missing 'orders' table
                SemanticTable(
                    name="products",  # Extra table not in source
                    description="Product catalog",
                    columns=[
                        SemanticColumn(name="product_id", data_type="INTEGER"),
                    ],
                ),
            ],
            measures=[],  # Missing measure
        )

    def test_detect_no_changes(self, detector):
        """Test detecting no changes between identical models."""
        model = SemanticModel(
            name="test",
            source="fabric",
            tables=[
                SemanticTable(
                    name="table1",
                    columns=[
                        SemanticColumn(name="col1", data_type="VARCHAR"),
                    ],
                ),
            ],
        )
        
        # Create identical copy
        model_copy = SemanticModel(
            name="test_copy",
            source="snowflake",
            tables=[
                SemanticTable(
                    name="table1",
                    columns=[
                        SemanticColumn(name="col1", data_type="VARCHAR"),
                    ],
                ),
            ],
        )

        report = detector.detect_changes(model, model_copy)
        
        assert not report.has_changes
        assert report.summary()["total"] == 0

    def test_detect_added_table(self, detector, source_model, target_model):
        """Test detecting added tables."""
        report = detector.detect_changes(source_model, target_model)
        
        # 'orders' table should be detected as added
        added_tables = [
            c for c in report.additions
            if c.entity_type == "table" and c.entity_name == "orders"
        ]
        assert len(added_tables) == 1

    def test_detect_removed_table(self, detector, source_model, target_model):
        """Test detecting removed tables."""
        report = detector.detect_changes(source_model, target_model)
        
        # 'products' table should be detected as removed (exists in target, not in source)
        removed_tables = [
            c for c in report.removals
            if c.entity_type == "table" and c.entity_name == "products"
        ]
        assert len(removed_tables) == 1

    def test_detect_modified_table(self, detector, source_model, target_model):
        """Test detecting modified table properties."""
        report = detector.detect_changes(source_model, target_model)
        
        # 'customers' table has different description
        modified_tables = [
            c for c in report.modifications
            if c.entity_type == "table" and c.entity_name == "customers"
        ]
        assert len(modified_tables) == 1

    def test_detect_added_column(self, detector, source_model, target_model):
        """Test detecting added columns."""
        report = detector.detect_changes(source_model, target_model)
        
        # 'email' column should be detected as added to customers
        added_columns = [
            c for c in report.additions
            if c.entity_type == "column" and c.entity_name == "email"
        ]
        assert len(added_columns) == 1
        assert added_columns[0].parent_entity == "customers"

    def test_detect_added_measure(self, detector, source_model, target_model):
        """Test detecting added measures."""
        report = detector.detect_changes(source_model, target_model)
        
        # 'total_orders' measure should be detected as added
        added_measures = [
            c for c in report.additions
            if c.entity_type == "measure" and c.entity_name == "total_orders"
        ]
        assert len(added_measures) == 1

    def test_case_insensitive_comparison(self):
        """Test case-insensitive name comparison."""
        detector = ChangeDetector(case_sensitive=False)
        
        source = SemanticModel(
            name="source",
            source="fabric",
            tables=[
                SemanticTable(name="CUSTOMERS", columns=[]),
            ],
        )
        
        target = SemanticModel(
            name="target",
            source="snowflake",
            tables=[
                SemanticTable(name="customers", columns=[]),
            ],
        )

        report = detector.detect_changes(source, target)
        
        # Tables should be considered the same (case-insensitive)
        table_changes = [c for c in report.changes if c.entity_type == "table"]
        # No additions or removals expected
        assert all(c.change_type != ChangeType.ADDED for c in table_changes)
        assert all(c.change_type != ChangeType.REMOVED for c in table_changes)

    def test_ignore_hidden_entities(self):
        """Test ignoring hidden entities in comparison."""
        detector = ChangeDetector(ignore_hidden=True)
        
        source = SemanticModel(
            name="source",
            source="fabric",
            tables=[
                SemanticTable(
                    name="visible_table",
                    columns=[],
                    is_hidden=False,
                ),
                SemanticTable(
                    name="hidden_table",
                    columns=[],
                    is_hidden=True,
                ),
            ],
        )
        
        target = SemanticModel(
            name="target",
            source="snowflake",
            tables=[
                SemanticTable(
                    name="visible_table",
                    columns=[],
                ),
            ],
        )

        report = detector.detect_changes(source, target)
        
        # Hidden table should not be detected as added
        added_tables = [c for c in report.additions if c.entity_type == "table"]
        assert len(added_tables) == 0


class TestDataTypeConversion:
    """Tests for DataType conversion methods."""

    def test_from_snowflake(self):
        """Test conversion from Snowflake types."""
        assert DataType.from_snowflake("VARCHAR") == DataType.STRING
        assert DataType.from_snowflake("INTEGER") == DataType.INTEGER
        assert DataType.from_snowflake("TIMESTAMP") == DataType.DATETIME
        assert DataType.from_snowflake("BOOLEAN") == DataType.BOOLEAN
        assert DataType.from_snowflake("VARCHAR(255)") == DataType.STRING

    def test_from_fabric(self):
        """Test conversion from Fabric types."""
        assert DataType.from_fabric("String") == DataType.STRING
        assert DataType.from_fabric("Int64") == DataType.INTEGER
        assert DataType.from_fabric("DateTime") == DataType.DATETIME
        assert DataType.from_fabric("Boolean") == DataType.BOOLEAN

    def test_to_snowflake(self):
        """Test conversion to Snowflake types."""
        assert DataType.STRING.to_snowflake() == "VARCHAR"
        assert DataType.INTEGER.to_snowflake() == "INTEGER"
        assert DataType.DATETIME.to_snowflake() == "TIMESTAMP"

    def test_to_fabric(self):
        """Test conversion to Fabric types."""
        assert DataType.STRING.to_fabric() == "String"
        assert DataType.INTEGER.to_fabric() == "Int64"
        assert DataType.DATETIME.to_fabric() == "DateTime"
