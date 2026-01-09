"""
Unit tests for the semantic formatter module.
"""

import pytest
import json

from semantic_sync.core.semantic_formatter import SemanticFormatter, OutputFormat
from semantic_sync.core.change_detector import Change, ChangeReport, ChangeType
from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
)


class TestSemanticFormatter:
    """Tests for SemanticFormatter class."""

    @pytest.fixture
    def sample_model(self):
        """Create a sample semantic model."""
        return SemanticModel(
            name="test_model",
            source="fabric",
            description="Test model for formatting",
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
                    ],
                ),
            ],
            measures=[
                SemanticMeasure(
                    name="Total Count",
                    expression="COUNT(*)",
                ),
            ],
        )

    @pytest.fixture
    def sample_report(self):
        """Create a sample change report."""
        changes = [
            Change(
                change_type=ChangeType.ADDED,
                entity_type="table",
                entity_name="orders",
            ),
            Change(
                change_type=ChangeType.MODIFIED,
                entity_type="column",
                entity_name="email",
                parent_entity="customers",
                details={
                    "description": {
                        "old": "Email address",
                        "new": "Primary email",
                    }
                },
            ),
            Change(
                change_type=ChangeType.REMOVED,
                entity_type="measure",
                entity_name="old_measure",
            ),
        ]
        return ChangeReport(
            source="source_model",
            target="target_model",
            changes=changes,
        )

    def test_formatter_creation(self):
        """Test creating a formatter with different options."""
        formatter = SemanticFormatter()
        assert formatter._format == OutputFormat.TABLE
        assert formatter._colorize is True
        
        formatter = SemanticFormatter(
            output_format=OutputFormat.JSON,
            colorize=False,
            verbose=True,
        )
        assert formatter._format == OutputFormat.JSON
        assert formatter._colorize is False
        assert formatter._verbose is True

    def test_format_model_table(self, sample_model):
        """Test formatting model as table."""
        formatter = SemanticFormatter(
            output_format=OutputFormat.TABLE,
            colorize=False,
        )
        output = formatter.format_model(sample_model)
        
        assert "test_model" in output
        assert "customers" in output
        assert "id" in output
        assert "name" in output

    def test_format_model_json(self, sample_model):
        """Test formatting model as JSON."""
        formatter = SemanticFormatter(output_format=OutputFormat.JSON)
        output = formatter.format_model(sample_model)
        
        # Should be valid JSON
        data = json.loads(output)
        assert data["name"] == "test_model"
        assert len(data["tables"]) == 1

    def test_format_model_markdown(self, sample_model):
        """Test formatting model as Markdown."""
        formatter = SemanticFormatter(output_format=OutputFormat.MARKDOWN)
        output = formatter.format_model(sample_model)
        
        assert "# Semantic Model: test_model" in output
        assert "## Tables" in output
        assert "| Column | Type |" in output

    def test_format_changes_table(self, sample_report):
        """Test formatting changes as table."""
        formatter = SemanticFormatter(
            output_format=OutputFormat.TABLE,
            colorize=False,
        )
        output = formatter.format_changes(sample_report)
        
        assert "Change Report" in output
        assert "Additions:" in output
        assert "Modifications:" in output
        assert "Removals:" in output
        assert "orders" in output
        assert "email" in output

    def test_format_changes_json(self, sample_report):
        """Test formatting changes as JSON."""
        formatter = SemanticFormatter(output_format=OutputFormat.JSON)
        output = formatter.format_changes(sample_report)
        
        data = json.loads(output)
        assert data["source"] == "source_model"
        assert data["target"] == "target_model"
        assert len(data["changes"]) == 3
        assert data["summary"]["added"] == 1
        assert data["summary"]["modified"] == 1
        assert data["summary"]["removed"] == 1

    def test_format_changes_markdown(self, sample_report):
        """Test formatting changes as Markdown."""
        formatter = SemanticFormatter(output_format=OutputFormat.MARKDOWN)
        output = formatter.format_changes(sample_report)
        
        assert "# Change Report" in output
        assert "## Summary" in output
        assert "### Additions" in output
        assert "### Modifications" in output
        assert "### Removals" in output

    def test_format_diff_added(self):
        """Test formatting a single added change as diff."""
        formatter = SemanticFormatter(colorize=False)
        change = Change(
            change_type=ChangeType.ADDED,
            entity_type="table",
            entity_name="new_table",
        )
        
        output = formatter.format_diff(change)
        assert "TABLE: new_table" in output
        assert "+ Added" in output

    def test_format_diff_modified(self):
        """Test formatting a modified change as diff."""
        formatter = SemanticFormatter(colorize=False)
        change = Change(
            change_type=ChangeType.MODIFIED,
            entity_type="column",
            entity_name="email",
            parent_entity="users",
            details={
                "description": {"old": "Old desc", "new": "New desc"}
            },
        )
        
        output = formatter.format_diff(change)
        assert "COLUMN: users.email" in output
        assert "~ Modified" in output
        assert "description" in output

    def test_format_diff_removed(self):
        """Test formatting a removed change as diff."""
        formatter = SemanticFormatter(colorize=False)
        change = Change(
            change_type=ChangeType.REMOVED,
            entity_type="measure",
            entity_name="old_measure",
        )
        
        output = formatter.format_diff(change)
        assert "MEASURE: old_measure" in output
        assert "- Removed" in output

    def test_no_changes_message(self):
        """Test message when no changes detected."""
        formatter = SemanticFormatter(colorize=False)
        report = ChangeReport(source="a", target="b", changes=[])
        
        output = formatter.format_changes(report)
        assert "No changes detected" in output or "in sync" in output
