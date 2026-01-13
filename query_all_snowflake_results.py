#!/usr/bin/env python3
"""
Query All Snowflake Results - Fabric Samples Sync
Shows all synced data from Snowflake account
"""

import os
import sys
from datetime import datetime
import json

sys.path.insert(0, ".")
sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
import snowflake.connector


def print_section(title):
    """Print a formatted section header."""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)


def query_all_models(conn):
    """Query all synced semantic models."""
    print_section("ALL SYNCED MODELS")
    
    query = """
    SELECT 
        MODEL_NAME,
        SOURCE_SYSTEM,
        TABLE_COUNT,
        SYNC_VERSION,
        CREATED_AT,
        UPDATED_AT
    FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_METADATA
    ORDER BY UPDATED_AT DESC
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"\nTotal Models: {len(results)}\n")
    print(f"{'#':<4} {'Model Name':<35} {'Source':<12} {'Tables':<8} {'Updated':<20}")
    print("-" * 80)
    
    for i, row in enumerate(results, 1):
        model_name = row[0][:35]
        source = row[1] or 'N/A'
        tables = row[2]
        updated = row[5].strftime('%Y-%m-%d %H:%M:%S') if row[5] else 'N/A'
        print(f"{i:<4} {model_name:<35} {source:<12} {tables:<8} {updated:<20}")
    
    cursor.close()
    return results


def query_sync_history(conn):
    """Query sync execution history."""
    print_section("SYNC HISTORY")
    
    query = """
    SELECT 
        SYNC_ID,
        RUN_ID,
        STARTED_AT,
        STATUS,
        CHANGES_APPLIED
    FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_SYNC_HISTORY
    ORDER BY STARTED_AT DESC
    LIMIT 10
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        results = cursor.fetchall()
        
        print(f"\nRecent Syncs (Last {len(results)}):\n")
        print(f"{'Sync ID':<30} {'Started':<20} {'Status':<12} {'Changes':<10}")
        print("-" * 80)
        
        for row in results:
            sync_id = row[0][:30] if row[0] else 'N/A'
            started = row[2].strftime('%Y-%m-%d %H:%M:%S') if row[2] else 'N/A'
            status = row[3] or 'N/A'
            changes = row[4] or 0
            print(f"{sync_id:<30} {started:<20} {status:<12} {changes:<10}")
        
        cursor.close()
        return results
    except Exception as e:
        print(f"  Error querying sync history: {e}")
        cursor.close()
        return []


def query_all_tables(conn):
    """Query all data tables in Snowflake."""
    print_section("ALL DATA TABLES")
    
    query = """
    SELECT 
        TABLE_NAME,
        ROW_COUNT,
        BYTES,
        CREATED as CREATED_AT
    FROM ANALYTICS_DB.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
      AND TABLE_TYPE = 'BASE TABLE'
      AND TABLE_NAME NOT LIKE '_SEMANTIC%'
    ORDER BY TABLE_NAME
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"\nData Tables: {len(results)}\n")
    print(f"{'Table Name':<30} {'Rows':<12} {'Size (KB)':<12} {'Created':<20}")
    print("-" * 80)
    
    for row in results:
        table_name = row[0]
        rows = row[1] or 0
        size_kb = (row[2] or 0) / 1024
        created = row[3].strftime('%Y-%m-%d %H:%M:%S') if row[3] else 'N/A'
        print(f"{table_name:<30} {rows:<12} {size_kb:<12.2f} {created:<20}")
    
    cursor.close()
    return results


def query_table_sample(conn, table_name, limit=5):
    """Query sample data from a table."""
    query = f"""
    SELECT *
    FROM ANALYTICS_DB.SEMANTIC_LAYER.{table_name}
    LIMIT {limit}
    """
    
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        results = cursor.fetchall()
        cursor.close()
        return columns, results
    except Exception as e:
        cursor.close()
        return None, None


def query_measures(conn):
    """Query all measures."""
    print_section("MEASURES")
    
    query = """
    SELECT 
        MEASURE_NAME,
        TABLE_NAME,
        EXPRESSION,
        DESCRIPTION
    FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_MEASURES
    ORDER BY TABLE_NAME, MEASURE_NAME
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"\nTotal Measures: {len(results)}\n")
    
    if results:
        for row in results:
            print(f"Measure: {row[0]}")
            print(f"  Table: {row[1]}")
            print(f"  Expression: {row[2][:60]}..." if len(row[2]) > 60 else f"  Expression: {row[2]}")
            print(f"  Description: {row[3] or 'N/A'}")
            print()
    else:
        print("  No measures found.\n")
    
    cursor.close()
    return results


def query_relationships(conn):
    """Query all relationships."""
    print_section("RELATIONSHIPS")
    
    query = """
    SELECT 
        FROM_TABLE,
        FROM_COLUMN,
        TO_TABLE,
        TO_COLUMN,
        CARDINALITY
    FROM ANALYTICS_DB.SEMANTIC_LAYER._SEMANTIC_RELATIONSHIPS
    ORDER BY FROM_TABLE
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    print(f"\nTotal Relationships: {len(results)}\n")
    
    if results:
        for row in results:
            print(f"  {row[0]}.{row[1]} -> {row[2]}.{row[3]} ({row[4]})")
    else:
        print("  No relationships found.\n")
    
    cursor.close()
    return results


def export_results_to_json(models, sync_history, tables, measures, relationships):
    """Export all results to JSON file."""
    results = {
        "export_timestamp": datetime.now().isoformat(),
        "snowflake_account": "FA97567.central-india.azure",
        "database": "ANALYTICS_DB",
        "schema": "SEMANTIC_LAYER",
        "summary": {
            "total_models": len(models),
            "total_tables": len(tables),
            "total_syncs": len(sync_history),
            "total_measures": len(measures),
            "total_relationships": len(relationships)
        },
        "models": [
            {
                "name": row[0],
                "source": row[1],
                "table_count": row[2],
                "updated_at": row[5].isoformat() if row[5] else None
            }
            for row in models
        ],
        "sync_history": [
            {
                "sync_id": row[0],
                "started_at": row[1].isoformat() if row[1] else None,
                "status": row[3],
                "changes_applied": row[4],
                "duration_seconds": row[7]
            }
            for row in sync_history
        ],
        "tables": [
            {
                "table_name": row[0],
                "row_count": row[1],
                "size_bytes": row[2],
                "created_at": row[3].isoformat() if row[3] else None
            }
            for row in tables
        ]
    }
    
    filename = f"snowflake_sync_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nResults exported to: {filename}")
    return filename


def main():
    """Main entry point."""
    
    print("\n" + "="*80)
    print("  SNOWFLAKE SYNC RESULTS - FABRIC SAMPLES")
    print("="*80)
    print(f"\n  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Account: FA97567.central-india.azure")
    print(f"  Database: ANALYTICS_DB")
    print(f"  Schema: SEMANTIC_LAYER\n")
    
    # Connect to Snowflake
    try:
        settings = get_settings()
        conf = settings.get_snowflake_config()
        
        conn = snowflake.connector.connect(
            account=conf.account,
            user=conf.user,
            password=conf.password.get_secret_value(),
            warehouse=conf.warehouse,
            database=conf.database,
            schema=conf.schema_name
        )
        
        # Query all data
        models = query_all_models(conn)
        sync_history = query_sync_history(conn)
        tables = query_all_tables(conn)
        measures = query_measures(conn)
        relationships = query_relationships(conn)
        
        # Show sample data from each table
        if tables:
            print_section("SAMPLE DATA FROM TABLES")
            for table_row in tables[:3]:  # Show first 3 tables
                table_name = table_row[0]
                print(f"\nTable: {table_name}")
                columns, data = query_table_sample(conn, table_name, limit=3)
                
                if columns and data:
                    print(f"Columns: {', '.join(columns)}")
                    print(f"Sample Rows ({len(data)}):")
                    for row in data:
                        print(f"  {row}")
                else:
                    print("  (No data or error accessing table)")
        
        # Export to JSON
        print_section("EXPORT")
        export_file = export_results_to_json(models, sync_history, tables, measures, relationships)
        
        # Final summary
        print_section("SUMMARY")
        print(f"""
  Models Synced:        {len(models)}
  Data Tables:          {len(tables)}
  Total Syncs:          {len(sync_history)}
  Measures:             {len(measures)}
  Relationships:        {len(relationships)}
  
  Export File:          {export_file}
        """)
        
        conn.close()
        
        print("\n" + "="*80)
        print("  ALL SYNCED DATA IS AVAILABLE IN YOUR SNOWFLAKE ACCOUNT")
        print("="*80)
        print("""
  Connect to Snowflake and run:
  
    USE DATABASE ANALYTICS_DB;
    USE SCHEMA SEMANTIC_LAYER;
    
    SELECT * FROM _SEMANTIC_METADATA;
    SELECT * FROM DEMO_PRODUCTS;
    SELECT * FROM SALES_DATA;
        """)
        print("="*80 + "\n")
        
    except Exception as e:
        print(f"\n[ERROR] Failed to query Snowflake: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
