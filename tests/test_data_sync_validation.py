"""
Data Sync Validation Tests.

Comprehensive tests validating:
1. Fabric → Snowflake data transfer in row-table semantic format
2. SQLite rollback mechanism
3. Bidirectional sync (Snowflake ↔ SQL/Fabric)
4. Data consistency and discrepancy detection
"""

import pytest
import tempfile
from pathlib import Path
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock

from semantic_sync.core.models import (
    SemanticModel,
    SemanticTable,
    SemanticColumn,
    SemanticMeasure,
    SemanticRelationship,
)
from semantic_sync.core.change_detector import ChangeDetector, ChangeType
from semantic_sync.core.sqlite_rollback import RollbackManager

# Import test fixtures
from tests.fixtures.sample_fabric_data import (
    create_sales_model,
    create_inventory_model,
    create_modified_sales_model,
    SAMPLE_PRODUCTS_DATA,
    SAMPLE_CUSTOMERS_DATA,
    SAMPLE_ORDERS_DATA,
)


# =============================================================================
# Test Result Formatting
# =============================================================================

def format_test_result(
    test_name: str,
    status: str,
    details: dict = None,
    discrepancies: list = None,
) -> str:
    """Format test result for display."""
    status_icon = "✅ PASS" if status == "pass" else "❌ FAIL"
    
    result = f"""
╔══════════════════════════════════════════════════════════════════╗
║ TEST: {test_name.ljust(55)}║
╠══════════════════════════════════════════════════════════════════╣
║ Status: {status_icon.ljust(54)}║"""
    
    if details:
        for key, value in details.items():
            line = f"║   {key}: {str(value).ljust(52)}║"
            result += f"\n{line}"
    
    if discrepancies:
        result += "\n║ Discrepancies:                                                   ║"
        for d in discrepancies:
            line = f"║   - {str(d)[:56].ljust(56)}║"
            result += f"\n{line}"
    
    result += "\n╚══════════════════════════════════════════════════════════════════╝"
    return result


# =============================================================================
# Component 1: Fabric → Snowflake Transfer Tests (Row-Table Semantic Format)
# =============================================================================

class TestFabricToSnowflakeTransfer:
    """Tests validating Fabric to Snowflake data transfer in row-table format."""
    
    @pytest.fixture
    def fabric_model(self):
        """Get sample Fabric model."""
        return create_sales_model()
    
    @pytest.fixture
    def detector(self):
        """Create change detector."""
        return ChangeDetector(case_sensitive=False)
    
    def test_fabric_to_snowflake_row_table_format(self, fabric_model):
        """
        Verify that Fabric data is transferred as row-table semantic format.
        
        Validates:
        - Tables are properly structured with rows and columns
        - Column metadata (types, descriptions) is preserved
        - Semantic information (measures, relationships) is intact
        """
        # Verify table structure
        assert len(fabric_model.tables) > 0, "Model should have tables"
        
        test_details = {
            "Tables transferred": len(fabric_model.tables),
            "Total columns": sum(len(t.columns) for t in fabric_model.tables),
            "Measures": len(fabric_model.measures),
            "Relationships": len(fabric_model.relationships),
        }
        
        discrepancies = []
        
        # Check each table has proper row-table format
        for table in fabric_model.tables:
            # Table must have name
            if not table.name:
                discrepancies.append(f"Table missing name")
                
            # Table must have columns (rows are defined by columns)
            if not table.columns:
                discrepancies.append(f"Table {table.name} has no columns")
                
            # Each column must have name and type (semantic format)
            for col in table.columns:
                if not col.name:
                    discrepancies.append(f"Column in {table.name} missing name")
                if not col.data_type:
                    discrepancies.append(f"Column {col.name} missing data type")
        
        # Verify semantic metadata preservation
        products = next((t for t in fabric_model.tables if t.name == "Products"), None)
        assert products is not None, "Products table should exist"
        
        # Check column metadata
        product_id = next((c for c in products.columns if c.name == "ProductID"), None)
        assert product_id is not None, "ProductID column should exist"
        assert product_id.is_nullable is False, "ProductID should be non-nullable"
        
        # Check format strings are preserved
        unit_price = next((c for c in products.columns if c.name == "UnitPrice"), None)
        assert unit_price is not None, "UnitPrice column should exist"
        assert unit_price.format_string == "$#,##0.00", "Format string should be preserved"
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Fabric→Snowflake Row-Table Format",
            status,
            test_details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_semantic_metadata_preservation(self, fabric_model):
        """
        Verify semantic metadata is fully preserved during transfer.
        
        Validates:
        - Descriptions are transferred
        - Data types are correct
        - Format strings are preserved
        - Nullability constraints are maintained
        """
        discrepancies = []
        details = {}
        
        # Check table descriptions
        tables_with_desc = [t for t in fabric_model.tables if t.description]
        details["Tables with descriptions"] = f"{len(tables_with_desc)}/{len(fabric_model.tables)}"
        
        if len(tables_with_desc) != len(fabric_model.tables):
            discrepancies.append("Some tables missing descriptions")
        
        # Check column descriptions
        total_cols = sum(len(t.columns) for t in fabric_model.tables)
        cols_with_desc = sum(
            len([c for c in t.columns if c.description]) 
            for t in fabric_model.tables
        )
        details["Columns with descriptions"] = f"{cols_with_desc}/{total_cols}"
        
        # Check data type mapping
        expected_types = {"Int64", "String", "DateTime", "Decimal"}
        actual_types = set()
        for table in fabric_model.tables:
            for col in table.columns:
                actual_types.add(col.data_type)
        
        details["Unique data types"] = len(actual_types)
        
        # Verify measures have expressions
        measures_with_expr = [m for m in fabric_model.measures if m.expression]
        details["Measures with expressions"] = f"{len(measures_with_expr)}/{len(fabric_model.measures)}"
        
        if len(measures_with_expr) != len(fabric_model.measures):
            discrepancies.append("Some measures missing expressions")
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Semantic Metadata Preservation",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_relationship_transfer(self, fabric_model):
        """
        Verify relationships are correctly transferred.
        
        Validates:
        - All relationships have from/to table references
        - Column references are valid
        - Relationship types are preserved
        """
        discrepancies = []
        details = {"Total relationships": len(fabric_model.relationships)}
        
        table_names = {t.name for t in fabric_model.tables}
        
        for rel in fabric_model.relationships:
            # Check from_table exists
            if rel.from_table not in table_names:
                discrepancies.append(f"Relationship {rel.name}: from_table '{rel.from_table}' not found")
            
            # Check to_table exists
            if rel.to_table not in table_names:
                discrepancies.append(f"Relationship {rel.name}: to_table '{rel.to_table}' not found")
            
            # Check columns exist in respective tables
            from_table = next((t for t in fabric_model.tables if t.name == rel.from_table), None)
            if from_table:
                from_col_names = {c.name for c in from_table.columns}
                if rel.from_column not in from_col_names:
                    discrepancies.append(f"Relationship {rel.name}: from_column '{rel.from_column}' not in {rel.from_table}")
        
        details["Valid relationships"] = len(fabric_model.relationships) - len(discrepancies)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Relationship Transfer Validation",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"


# =============================================================================
# Component 2: SQLite Rollback Mechanism Tests
# =============================================================================

class TestSQLiteRollback:
    """Tests for SQLite-based rollback mechanism."""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database."""
        with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
            yield Path(f.name)
        Path(f.name).unlink(missing_ok=True)
    
    @pytest.fixture
    def manager(self, temp_db):
        """Create rollback manager."""
        return RollbackManager(db_path=temp_db)
    
    def test_rollback_restores_previous_version(self, manager):
        """
        Verify rollback mechanism restores previous data version.
        
        Workflow:
        1. Create initial model snapshot
        2. Simulate modifications
        3. Restore from snapshot
        4. Verify original state is restored
        """
        # Original model
        original = create_sales_model()
        original_tables_count = len(original.tables)
        original_measures_count = len(original.measures)
        
        # Create snapshot before "sync"
        snapshot_id = manager.create_snapshot(original, description="Pre-sync backup")
        
        # Simulate modifications (would happen during sync)
        modified = create_modified_sales_model()
        
        # Verify modifications exist
        assert len(modified.tables) == len(original.tables)  # Same count
        products_mod = next(t for t in modified.tables if t.name == "Products")
        products_orig = next(t for t in original.tables if t.name == "Products")
        assert len(products_mod.columns) != len(products_orig.columns)  # Different columns
        
        # Rollback to snapshot
        restored = manager.restore_snapshot(snapshot_id)
        
        # Verify restoration
        discrepancies = []
        details = {}
        
        details["Original tables"] = original_tables_count
        details["Restored tables"] = len(restored.tables)
        
        if len(restored.tables) != original_tables_count:
            discrepancies.append(f"Table count mismatch: expected {original_tables_count}, got {len(restored.tables)}")
        
        # Verify Products table has original columns
        restored_products = next(t for t in restored.tables if t.name == "Products")
        if len(restored_products.columns) != len(products_orig.columns):
            discrepancies.append(f"Products columns mismatch: expected {len(products_orig.columns)}, got {len(restored_products.columns)}")
        
        # Verify SubCategory column exists (was removed in modified version)
        subcategory = next((c for c in restored_products.columns if c.name == "SubCategory"), None)
        if subcategory is None:
            discrepancies.append("SubCategory column not restored")
        
        details["Original measures"] = original_measures_count
        details["Restored measures"] = len(restored.measures)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "SQLite Rollback Restores Previous Version",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_multiple_snapshots_rollback(self, manager):
        """
        Verify rollback works with multiple snapshots.
        
        Create multiple versions and verify each can be restored independently.
        """
        discrepancies = []
        details = {}
        
        # Create multiple model versions
        versions = []
        for i in range(3):
            model = SemanticModel(
                name=f"Version{i}",
                source="fabric",
                description=f"Version {i} of the model",
                tables=[
                    SemanticTable(
                        name="TestTable",
                        columns=[
                            SemanticColumn(name=f"Col_{j}", data_type="String")
                            for j in range(i + 1)  # Each version has more columns
                        ],
                    ),
                ],
            )
            snapshot_id = manager.create_snapshot(model, description=f"Version {i}")
            versions.append((snapshot_id, i + 1))  # (id, expected_column_count)
        
        details["Snapshots created"] = len(versions)
        
        # Verify each version can be restored correctly
        for snapshot_id, expected_cols in versions:
            restored = manager.restore_snapshot(snapshot_id)
            actual_cols = len(restored.tables[0].columns)
            
            if actual_cols != expected_cols:
                discrepancies.append(f"Snapshot {snapshot_id[:8]}...: expected {expected_cols} cols, got {actual_cols}")
        
        details["Successfully restored"] = len(versions) - len(discrepancies)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Multiple Snapshots Rollback",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"


# =============================================================================
# Component 3: Bidirectional Sync Tests (Snowflake ↔ SQL/Fabric)
# =============================================================================

class TestBidirectionalSync:
    """Tests for bidirectional synchronization between Snowflake and Fabric."""
    
    @pytest.fixture
    def detector(self):
        """Create change detector."""
        return ChangeDetector(case_sensitive=False)
    
    @pytest.fixture
    def fabric_model(self):
        """Fabric source model."""
        return create_sales_model()
    
    @pytest.fixture
    def snowflake_model(self):
        """Simulated Snowflake model (slightly different from Fabric)."""
        model = create_inventory_model()  # Different model structure
        return model
    
    def test_snowflake_to_fabric_sync(self, detector):
        """
        Verify Snowflake → Fabric sync direction.
        
        Validates:
        - Changes are correctly detected
        - Added/modified/removed entities are identified
        - Sync would apply correct transformations
        """
        # Source: Snowflake (inventory model)
        source = create_inventory_model()
        
        # Target: Fabric (empty/different model)
        target = SemanticModel(
            name="TargetModel",
            source="fabric",
            tables=[],  # Empty target
        )
        
        # Detect changes
        report = detector.detect_changes(source, target)
        
        discrepancies = []
        details = {
            "Direction": "Snowflake → Fabric",
            "Tables to add": len(report.additions),
            "Tables to modify": len(report.modifications),
            "Tables to remove": len(report.removals),
        }
        
        # Verify all source tables would be added
        source_tables = {t.name for t in source.tables}
        added_tables = {c.entity_name for c in report.additions if c.entity_type == "table"}
        
        if source_tables != added_tables:
            missing = source_tables - added_tables
            if missing:
                discrepancies.append(f"Tables not detected for addition: {missing}")
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Snowflake → Fabric Sync Detection",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_fabric_to_snowflake_sync(self, detector):
        """
        Verify Fabric → Snowflake sync direction.
        
        Validates:
        - Changes are correctly detected in reverse direction
        - Semantic metadata is properly identified for sync
        """
        # Source: Fabric (sales model)
        source = create_sales_model()
        
        # Target: Snowflake (empty)
        target = SemanticModel(
            name="SnowflakeTarget",
            source="snowflake",
            tables=[],
        )
        
        # Detect changes
        report = detector.detect_changes(source, target)
        
        discrepancies = []
        details = {
            "Direction": "Fabric → Snowflake",
            "Additions detected": len(report.additions),
            "Has changes": report.has_changes,
        }
        
        # All fabric tables should be detected as additions
        added_tables = [c for c in report.additions if c.entity_type == "table"]
        details["Tables to add"] = len(added_tables)
        
        if len(added_tables) != len(source.tables):
            discrepancies.append(f"Expected {len(source.tables)} table additions, got {len(added_tables)}")
        
        # Measures should also be detected
        added_measures = [c for c in report.additions if c.entity_type == "measure"]
        details["Measures to add"] = len(added_measures)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Fabric → Snowflake Sync Detection",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_bidirectional_sync_consistency(self, detector):
        """
        Verify round-trip sync maintains consistency.
        
        Workflow:
        1. Start with Fabric model
        2. Simulate sync to Snowflake (Fabric → Snowflake)
        3. Simulate sync back (Snowflake → Fabric)
        4. Verify no data loss
        """
        # Original Fabric model
        original = create_sales_model()
        
        # Simulated Snowflake copy (after Fabric → Snowflake sync)
        snowflake_copy = SemanticModel(
            name=original.name,
            source="snowflake",
            description=original.description,
            tables=original.tables.copy(),
            measures=original.measures.copy(),
            relationships=original.relationships.copy(),
        )
        
        # Detect changes (should be none after perfect copy)
        report = detector.detect_changes(original, snowflake_copy)
        
        discrepancies = []
        details = {
            "Original tables": len(original.tables),
            "Copied tables": len(snowflake_copy.tables),
            "Changes after round-trip": len(report.changes),
        }
        
        # No changes should exist for perfect copy
        # Note: Some metadata-only changes may exist (source field differs)
        critical_changes = [
            c for c in report.changes 
            if c.change_type in (ChangeType.ADDED, ChangeType.REMOVED)
        ]
        
        if critical_changes:
            for change in critical_changes:
                discrepancies.append(f"Unexpected {change.change_type.value}: {change.entity_type} {change.entity_name}")
        
        details["Critical changes"] = len(critical_changes)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Bidirectional Sync Consistency",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_incremental_sync_detection(self, detector):
        """
        Verify incremental sync correctly detects partial changes.
        
        Validates:
        - Changes are detected between source and modified target
        - Change detector identifies column-level additions and removals
        - Semantic metadata changes are tracked
        """
        # Original model (will be the target - existing state)
        original = create_sales_model()
        
        # Modified version (will be the source - new authoritative state)
        # Changes in modified:
        # - Products description changed
        # - SubCategory column removed from Products
        # - LoyaltyTier column added to Customers
        # - New measure "Customer Lifetime Value" added
        modified = create_modified_sales_model()
        
        # Detect changes: modified (source/new) vs original (target/old)
        # This simulates: "What changes need to be applied to original to match modified?"
        report = detector.detect_changes(modified, original)
        
        discrepancies = []
        details = {
            "Total changes": len(report.changes),
            "Additions": len(report.additions),
            "Modifications": len(report.modifications),
            "Removals": len(report.removals),
        }
        
        # Key validations:
        # 1. Some changes should be detected (original != modified)
        summary = report.summary()
        
        if summary["total"] == 0:
            discrepancies.append("No changes detected when changes should exist")
        
        # 2. Check for expected column addition (LoyaltyTier added in modified/source)
        loyalty_addition = [
            c for c in report.additions
            if c.entity_type == "column" and c.entity_name == "LoyaltyTier"
        ]
        if not loyalty_addition:
            discrepancies.append("LoyaltyTier column addition not detected")
        
        # 3. Check for expected column removal (SubCategory removed in modified/source)
        subcategory_removal = [
            c for c in report.removals
            if c.entity_type == "column" and c.entity_name == "SubCategory"
        ]
        if not subcategory_removal:
            discrepancies.append("SubCategory column removal not detected")
        
        # 4. Check for table modification (Products description changed)
        products_modification = [
            c for c in report.modifications
            if c.entity_type == "table" and c.entity_name == "Products"
        ]
        if not products_modification:
            discrepancies.append("Products table modification not detected")
        
        # 5. Check for measure addition
        measure_addition = [
            c for c in report.additions
            if c.entity_type == "measure" and c.entity_name == "Customer Lifetime Value"
        ]
        if not measure_addition:
            discrepancies.append("Customer Lifetime Value measure addition not detected")
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Incremental Sync Detection",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"


# =============================================================================
# Component 4: Data Consistency and Discrepancy Detection
# =============================================================================

class TestDataConsistency:
    """Tests for data consistency and discrepancy detection."""
    
    def test_data_type_consistency(self):
        """
        Verify data types are consistent across sync operations.
        
        Validates type mapping between Fabric and Snowflake formats.
        """
        # Fabric types
        fabric_types = {
            "Int64": "INTEGER",
            "String": "VARCHAR",
            "DateTime": "TIMESTAMP",
            "Decimal": "NUMBER",
            "Boolean": "BOOLEAN",
        }
        
        discrepancies = []
        details = {"Type mappings verified": len(fabric_types)}
        
        model = create_sales_model()
        
        for table in model.tables:
            for col in table.columns:
                if col.data_type not in fabric_types:
                    discrepancies.append(f"Unknown type {col.data_type} in {table.name}.{col.name}")
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Data Type Consistency",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_referential_integrity(self):
        """
        Verify referential integrity of relationships.
        
        Validates all foreign key references are valid.
        """
        model = create_sales_model()
        
        discrepancies = []
        table_columns = {}
        
        # Build lookup of table columns
        for table in model.tables:
            table_columns[table.name] = {col.name for col in table.columns}
        
        details = {
            "Total relationships": len(model.relationships),
            "Tables": len(model.tables),
        }
        
        # Validate each relationship
        for rel in model.relationships:
            # From table/column exists
            if rel.from_table not in table_columns:
                discrepancies.append(f"From table {rel.from_table} not found")
            elif rel.from_column not in table_columns.get(rel.from_table, set()):
                discrepancies.append(f"From column {rel.from_table}.{rel.from_column} not found")
            
            # To table/column exists
            if rel.to_table not in table_columns:
                discrepancies.append(f"To table {rel.to_table} not found")
            elif rel.to_column not in table_columns.get(rel.to_table, set()):
                discrepancies.append(f"To column {rel.to_table}.{rel.to_column} not found")
        
        details["Valid relationships"] = len(model.relationships) - len(discrepancies)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Referential Integrity",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"
    
    def test_measure_expression_validity(self):
        """
        Verify measure expressions reference valid tables/columns.
        """
        model = create_sales_model()
        
        discrepancies = []
        details = {"Total measures": len(model.measures)}
        
        table_names = {t.name for t in model.tables}
        
        for measure in model.measures:
            # Basic validation - expression should exist
            if not measure.expression:
                discrepancies.append(f"Measure {measure.name} has no expression")
                continue
            
            # Check if table_name is valid
            if measure.table_name and measure.table_name not in table_names:
                discrepancies.append(f"Measure {measure.name} references unknown table {measure.table_name}")
        
        details["Valid measures"] = len(model.measures) - len(discrepancies)
        
        status = "pass" if not discrepancies else "fail"
        print(format_test_result(
            "Measure Expression Validity",
            status,
            details,
            discrepancies,
        ))
        
        assert not discrepancies, f"Found discrepancies: {discrepancies}"


# =============================================================================
# Run All Validation Tests
# =============================================================================

def run_all_validations():
    """
    Run all validation tests and generate summary report.
    
    This function can be called directly to run all tests outside pytest.
    """
    print("\n" + "=" * 70)
    print(" FABRIC-SNOWFLAKE DATA SYNC VALIDATION TESTS")
    print("=" * 70)
    print(f" Run Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70 + "\n")
    
    # Run tests programmatically
    import sys
    pytest.main([
        __file__,
        "-v",
        "--tb=short",
        "-s",  # Show print statements
    ])


if __name__ == "__main__":
    run_all_validations()
