"""
Unit tests for the semantic models module.
"""

import pytest
from datetime import datetime

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
    DataType,
)


class TestSemanticColumn:
    """Tests for SemanticColumn model."""

    def test_column_creation(self):
        """Test creating a semantic column."""
        col = SemanticColumn(
            name="customer_id",
            data_type="INTEGER",
            is_nullable=False,
            description="Primary key",
        )
        
        assert col.name == "customer_id"
        assert col.data_type == "INTEGER"
        assert col.is_nullable is False
        assert col.description == "Primary key"

    def test_column_defaults(self):
        """Test default values for semantic column."""
        col = SemanticColumn(name="test", data_type="VARCHAR")
        
        assert col.is_nullable is True
        assert col.is_hidden is False
        assert col.description == ""
        assert col.format_string is None

    def test_column_normalized_type_inference(self):
        """Test automatic normalized type inference."""
        col = SemanticColumn(name="test", data_type="VARCHAR")
        assert col.normalized_type == DataType.STRING


class TestSemanticTable:
    """Tests for SemanticTable model."""

    def test_table_creation(self):
        """Test creating a semantic table."""
        table = SemanticTable(
            name="customers",
            description="Customer master data",
            columns=[
                SemanticColumn(name="id", data_type="INTEGER"),
                SemanticColumn(name="name", data_type="VARCHAR"),
            ],
        )
        
        assert table.name == "customers"
        assert table.description == "Customer master data"
        assert len(table.columns) == 2

    def test_table_defaults(self):
        """Test default values for semantic table."""
        table = SemanticTable(name="test")
        
        assert table.columns == []
        assert table.is_hidden is False
        assert table.source_table is None


class TestSemanticMeasure:
    """Tests for SemanticMeasure model."""

    def test_measure_creation(self):
        """Test creating a semantic measure."""
        measure = SemanticMeasure(
            name="Total Sales",
            expression="SUM(sales[amount])",
            description="Sum of all sales amounts",
            format_string="$#,##0.00",
        )
        
        assert measure.name == "Total Sales"
        assert measure.expression == "SUM(sales[amount])"
        assert measure.description == "Sum of all sales amounts"
        assert measure.format_string == "$#,##0.00"

    def test_measure_defaults(self):
        """Test default values for semantic measure."""
        measure = SemanticMeasure(
            name="test",
            expression="1+1",
        )
        
        assert measure.is_hidden is False
        assert measure.folder is None
        assert measure.data_type == "decimal"


class TestSemanticRelationship:
    """Tests for SemanticRelationship model."""

    def test_relationship_creation(self):
        """Test creating a semantic relationship."""
        rel = SemanticRelationship(
            name="orders_customers",
            from_table="orders",
            from_column="customer_id",
            to_table="customers",
            to_column="id",
            cardinality="many-to-one",
        )
        
        assert rel.name == "orders_customers"
        assert rel.from_table == "orders"
        assert rel.to_table == "customers"
        assert rel.cardinality == "many-to-one"

    def test_relationship_defaults(self):
        """Test default values for semantic relationship."""
        rel = SemanticRelationship(
            name="test",
            from_table="a",
            from_column="id",
            to_table="b",
            to_column="id",
        )
        
        assert rel.cardinality == "many-to-one"
        assert rel.cross_filter_direction == "single"
        assert rel.is_active is True


class TestSemanticModel:
    """Tests for SemanticModel model."""

    @pytest.fixture
    def sample_model(self):
        """Create a sample semantic model."""
        return SemanticModel(
            name="sales_model",
            source="fabric",
            description="Sales analytics model",
            tables=[
                SemanticTable(
                    name="customers",
                    columns=[
                        SemanticColumn(name="id", data_type="INTEGER"),
                        SemanticColumn(name="name", data_type="VARCHAR"),
                    ],
                ),
                SemanticTable(
                    name="orders",
                    columns=[
                        SemanticColumn(name="order_id", data_type="INTEGER"),
                        SemanticColumn(name="customer_id", data_type="INTEGER"),
                        SemanticColumn(name="amount", data_type="DECIMAL"),
                    ],
                ),
            ],
            measures=[
                SemanticMeasure(
                    name="Total Orders",
                    expression="COUNT(orders[order_id])",
                ),
            ],
            relationships=[
                SemanticRelationship(
                    name="orders_to_customers",
                    from_table="orders",
                    from_column="customer_id",
                    to_table="customers",
                    to_column="id",
                ),
            ],
        )

    def test_model_creation(self, sample_model):
        """Test creating a semantic model."""
        assert sample_model.name == "sales_model"
        assert sample_model.source == "fabric"
        assert len(sample_model.tables) == 2
        assert len(sample_model.measures) == 1
        assert len(sample_model.relationships) == 1

    def test_model_defaults(self):
        """Test default values for semantic model."""
        model = SemanticModel(name="test", source="snowflake")
        
        assert model.tables == []
        assert model.measures == []
        assert model.relationships == []
        assert model.metadata == {}
        assert model.version == "1.0"
        assert isinstance(model.extracted_at, datetime)

    def test_get_table(self, sample_model):
        """Test getting a table by name."""
        table = sample_model.get_table("customers")
        assert table is not None
        assert table.name == "customers"
        
        # Case-insensitive
        table = sample_model.get_table("CUSTOMERS")
        assert table is not None
        
        # Non-existent
        assert sample_model.get_table("nonexistent") is None

    def test_get_measure(self, sample_model):
        """Test getting a measure by name."""
        measure = sample_model.get_measure("Total Orders")
        assert measure is not None
        assert measure.name == "Total Orders"
        
        # Non-existent
        assert sample_model.get_measure("nonexistent") is None

    def test_get_relationship(self, sample_model):
        """Test getting a relationship by name."""
        rel = sample_model.get_relationship("orders_to_customers")
        assert rel is not None
        
        # Non-existent
        assert sample_model.get_relationship("nonexistent") is None

    def test_count_methods(self, sample_model):
        """Test count helper methods."""
        assert sample_model.table_count() == 2
        assert sample_model.column_count() == 5  # 2 + 3
        assert sample_model.measure_count() == 1
        assert sample_model.relationship_count() == 1

    def test_model_serialization(self, sample_model):
        """Test model serialization to dict."""
        data = sample_model.model_dump()
        
        assert data["name"] == "sales_model"
        assert data["source"] == "fabric"
        assert len(data["tables"]) == 2
        assert len(data["measures"]) == 1
        assert len(data["relationships"]) == 1
