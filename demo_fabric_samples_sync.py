#!/usr/bin/env python3
"""
Demo: Microsoft Fabric Samples to Snowflake Sync (Windows-safe version)

This script demonstrates syncing semantic models from Microsoft's fabric-samples
repository to Snowflake using ASCII-only characters for Windows compatibility.
"""

from __future__ import annotations

import sys
import os

# Setup path
sys.path.insert(0, ".")

from tests.fixtures.sample_fabric_data import (
    create_sales_model,
    create_inventory_model,
    SAMPLE_PRODUCTS_DATA,
    SAMPLE_CUSTOMERS_DATA,
    SAMPLE_ORDERS_DATA,
)
from semantic_sync.utils.logger import setup_logging


def print_banner():
    """Print demo banner."""
    banner = """
+======================================================================+
|                                                                      |
|   MICROSOFT FABRIC SAMPLES -> SNOWFLAKE SYNC DEMO                   |
|                                                                      |
|   Demonstrating semantic model synchronization from                 |
|   github.com/microsoft/fabric-samples to Snowflake                  |
|                                                                      |
+======================================================================+
"""
    print(banner)


def print_separator():
    """Print a separator line."""
    print("\n" + "="*70)


def print_model_summary(model):
    """Print a summary of the semantic model."""
    print(f"\n[*] Semantic Model: {model.name}")
    print(f"  Tables: {len(model.tables)} - {', '.join([t.name for t in model.tables])}")
    print(f"  Total Columns: {sum(len(t.columns) for t in model.tables)}")
    print(f"  Measures: {len(model.measures)} - {', '.join([m.name for m in model.measures])}")
    print(f"  Relationships: {len(model.relationships)}")


def print_table_details(table):
    """Print detailed table information."""
    print(f"\n[*] Table: {table.name}")
    print(f"  Description: {table.description}")
    print("  Columns:")
    for col in table.columns:
        nullable = " (nullable)" if col.is_nullable else ""
        desc = col.description or "N/A"
        print(f"    - {col.name} ({col.data_type}){nullable}: {desc}")


def demo_sample_models():
    """
    Demonstrate the fabric-samples models available for sync.
    """
    print_banner()
    
    print("\n[*] Available Sample Models from fabric-samples\n")
    
    # Model 1: Sales Analytics
    print_separator()
    print("  MODEL 1: SALES ANALYTICS")
    print_separator()
    
    sales_model = create_sales_model()
    print_model_summary(sales_model)
    
    # Show a sample table in detail
    print("\n[*] Sample Table Details:")
    products_table = next(t for t in sales_model.tables if t.name == "Products")
    print_table_details(products_table)
    
    # Show measures
    print("\n[*] Sample Measures:")
    for measure in sales_model.measures[:3]:  # Show first 3
        expr = measure.expression[:60] + "..." if len(measure.expression) > 60 else measure.expression
        print(f"  - {measure.name}")
        print(f"    Expression: {expr}")
        print(f"    Description: {measure.description or 'N/A'}")
    
    # Show relationships
    print("\n[*] Relationships:")
    for rel in sales_model.relationships:
        print(f"  {rel.from_table}.{rel.from_column} -> {rel.to_table}.{rel.to_column} ({rel.cardinality})")
    
    # Model 2: Inventory Management
    print_separator()
    print("  MODEL 2: INVENTORY MANAGEMENT")
    print_separator()
    
    inventory_model = create_inventory_model()
    print_model_summary(inventory_model)
    
    # Sample data preview
    print_separator()
    print("  SAMPLE DATA PREVIEW")
    print_separator()
    
    print("\n[*] Sample Products Data:")
    print(f"  {'ID':<5} {'Product Name':<20} {'Category':<15} {'Price':<10}")
    print(f"  {'-'*5} {'-'*20} {'-'*15} {'-'*10}")
    for product in SAMPLE_PRODUCTS_DATA[:3]:
        print(f"  {product['ProductID']:<5} "
              f"{product['ProductName']:<20} "
              f"{product['Category']:<15} "
              f"${product['UnitPrice']:>8.2f}")
    
    return sales_model, inventory_model


def demo_sync_to_snowflake(model):
    """
    Demonstrate syncing a model to Snowflake.
    """
    print_separator()
    print(f"  SYNCING {model.name} TO SNOWFLAKE")
    print_separator()
    
    # Simulated sync summary
    sync_summary = {
        "tables_to_create": len(model.tables),
        "columns_to_create": sum(len(t.columns) for t in model.tables),
        "measures_to_create": len(model.measures),
        "relationships_to_create": len(model.relationships),
        "metadata_tables": [
            "_SEMANTIC_METADATA",
            "_SEMANTIC_MEASURES", 
            "_SEMANTIC_RELATIONSHIPS",
            "_SEMANTIC_SYNC_HISTORY"
        ]
    }
    
    print("\n[*] Sync Summary:")
    print(f"  Tables to Create: {sync_summary['tables_to_create']}")
    print(f"  Columns to Create: {sync_summary['columns_to_create']}")
    print(f"  Measures to Create: {sync_summary['measures_to_create']}")
    print(f"  Relationships to Create: {sync_summary['relationships_to_create']}")
    print(f"  Metadata Tables: {', '.join(sync_summary['metadata_tables'])}")
    
    print("\n[!] DRY RUN MODE - No changes applied")
    
    # Show what would be in Snowflake
    print_separator()
    print("  SNOWFLAKE STRUCTURE (after sync)")
    print_separator()
    
    print(f"\nDatabase: ANALYTICS_DB")
    print(f"Schema: SEMANTIC_LAYER")
    print(f"\nTables Created:")
    for table in model.tables:
        print(f"  [TABLE] {table.name} ({len(table.columns)} columns)")
    
    print(f"\nMetadata Tables:")
    for meta_table in sync_summary["metadata_tables"]:
        print(f"  [META] {meta_table}")
    
    print(f"\nViews Created (from Measures):")
    for measure in model.measures:
        view_name = measure.name.replace(" ", "_").upper()
        print(f"  [VIEW] VW_{view_name}")


def main():
    """Main entry point."""
    setup_logging(level="INFO")
    
    # Show available models
    sales_model, inventory_model = demo_sample_models()
    
    # Demonstrate sync
    print_separator()
    print("  SYNC DEMONSTRATION")
    print_separator()
    
    demo_sync_to_snowflake(sales_model)
    
    # Final summary
    print_separator()
    print("  WHAT'S NEXT?")
    print_separator()
    print("""
To run an actual sync to Snowflake:

1. Configure your .env file with Fabric and Snowflake credentials
2. Run the demo in live mode:
   
   python demo_fabric_to_snowflake.py
   
3. Or use the CLI tool:
   
   semantic-sync fabric-to-sf --mode metadata-only
   
4. Verify in Snowflake:
   
   SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_METADATA;
   SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_MEASURES;
   SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_RELATIONSHIPS;
    """)
    
    print_separator()
    print("  [SUCCESS] Demo Complete!")
    print_separator()
    print("\nThe SalesAnalytics and InventoryManagement models are ready to sync.")
    print("Run with your Fabric credentials to push to Snowflake.\n")


if __name__ == "__main__":
    main()
