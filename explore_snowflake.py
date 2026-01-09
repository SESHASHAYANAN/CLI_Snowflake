"""
Script to explore what's in your Snowflake database and help set up the semantic view.
"""

import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")

from semantic_sync.config import get_settings
import snowflake.connector

def main():
    """Explore Snowflake database."""
    print("="*60)
    print("Snowflake Database Explorer")
    print("="*60)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        snowflake_config = settings.get_snowflake_config()
        
        print(f"Account: {snowflake_config.account}")
        print(f"Database: {snowflake_config.database}")
        print(f"Schema: {snowflake_config.schema_name}")
        print(f"Warehouse: {snowflake_config.warehouse}")
        print()
        
        # Connect to Snowflake
        print("Connecting to Snowflake...")
        conn = snowflake.connector.connect(
            account=snowflake_config.account,
            user=snowflake_config.user,
            password=snowflake_config.password.get_secret_value(),
            warehouse=snowflake_config.warehouse,
            database=snowflake_config.database,
            schema=snowflake_config.schema_name
        )
        
        print("[OK] Connected successfully!")
        print()
        
        cursor = conn.cursor()
        
        # Initialize variables
        tables = []
        views = []
        
        # Check if database exists
        print("="*60)
        print("Checking Database...")
        print("="*60)
        try:
            cursor.execute(f"SHOW DATABASES LIKE '{snowflake_config.database}'")
            dbs = cursor.fetchall()
            if dbs:
                print(f"[OK] Database '{snowflake_config.database}' exists")
            else:
                print(f"[WARNING] Database '{snowflake_config.database}' NOT found")
                print(f"          You may need to create it:")
                print(f"          CREATE DATABASE {snowflake_config.database};")
        except Exception as e:
            print(f"[ERROR] {e}")
        print()
        
        # Check if schema exists
        print("="*60)
        print("Checking Schema...")
        print("="*60)
        try:
            cursor.execute(f"SHOW SCHEMAS LIKE '{snowflake_config.schema_name}' IN DATABASE {snowflake_config.database}")
            schemas = cursor.fetchall()
            if schemas:
                print(f"[OK] Schema '{snowflake_config.schema_name}' exists")
            else:
                print(f"[WARNING] Schema '{snowflake_config.schema_name}' NOT found")
                print(f"          You may need to create it:")
                print(f"          CREATE SCHEMA {snowflake_config.database}.{snowflake_config.schema_name};")
        except Exception as e:
            print(f"[ERROR] {e}")
        print()
        
        # List all tables in the schema
        print("="*60)
        print("Tables in Schema...")
        print("="*60)
        try:
            cursor.execute(f"SHOW TABLES IN SCHEMA {snowflake_config.database}.{snowflake_config.schema_name}")
            tables = cursor.fetchall()
            
            if tables:
                print(f"[OK] Found {len(tables)} table(s):")
                for idx, table in enumerate(tables, 1):
                    table_name = table[1]  # Column 1 is table name
                    print(f"  {idx}. {table_name}")
            else:
                print("[INFO] No tables found in this schema")
        except Exception as e:
            print(f"[ERROR] {e}")
        print()
        
        # List all views in the schema
        print("="*60)
        print("Views in Schema...")
        print("="*60)
        try:
            cursor.execute(f"SHOW VIEWS IN SCHEMA {snowflake_config.database}.{snowflake_config.schema_name}")
            views = cursor.fetchall()
            
            if views:
                print(f"[OK] Found {len(views)} view(s):")
                for idx, view in enumerate(views, 1):
                    view_name = view[1]  # Column 1 is view name
                    print(f"  {idx}. {view_name}")
                    
                    # Check if it's the semantic view
                    if view_name == snowflake_config.semantic_view_name:
                        print(f"      [*] This is your configured SEMANTIC_VIEW!")
            else:
                print("[INFO] No views found in this schema")
                print()
                print(f"[TIP] The semantic-sync tool expects a view named: {snowflake_config.semantic_view_name}")
        except Exception as e:
            print(f"[ERROR] {e}")
        print()
        
        # Recommendation
        print("="*60)
        print("RECOMMENDATION")
        print("="*60)
        print()
        
        if not tables and not views:
            print("Your Snowflake schema is empty!")
            print()
            print("To test Snowflake -> Fabric sync, you need to:")
            print()
            print("1. Create a sample table:")
            print(f"   USE DATABASE {snowflake_config.database};")
            print(f"   USE SCHEMA {snowflake_config.schema_name};")
            print()
            print("   CREATE TABLE sample_data (")
            print("       id INTEGER,")
            print("       name VARCHAR(255),")
            print("       value DECIMAL(10,2),")
            print("       created_date DATE")
            print("   );")
            print()
            print("2. Create the semantic view:")
            print(f"   CREATE VIEW {snowflake_config.semantic_view_name} AS")
            print("   SELECT")
            print("       'sample_data' as table_name,")
            print("       column_name,")
            print("       data_type")
            print("   FROM information_schema.columns")
            print(f"   WHERE table_schema = '{snowflake_config.schema_name}'")
            print(f"   AND table_catalog = '{snowflake_config.database}';")
            print()
        elif not views or snowflake_config.semantic_view_name not in [v[1] for v in views]:
            print(f"You have tables but no '{snowflake_config.semantic_view_name}' view!")
            print()
            print("Create the semantic view to expose your tables:")
            print()
            print(f"   USE DATABASE {snowflake_config.database};")
            print(f"   USE SCHEMA {snowflake_config.schema_name};")
            print()
            print(f"   CREATE VIEW {snowflake_config.semantic_view_name} AS")
            print("   SELECT")
            print("       table_name,")
            print("       column_name,")
            print("       data_type,")
            print("       is_nullable")
            print("   FROM information_schema.columns")
            print(f"   WHERE table_schema = '{snowflake_config.schema_name}'")
            print(f"   AND table_catalog = '{snowflake_config.database}';")
        else:
            print("[OK] Your Snowflake setup looks good!")
            print()
            print("You should be able to run:")
            print("  python -m semantic_sync.main preview --direction snowflake-to-fabric")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
