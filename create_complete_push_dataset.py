"""
Create Push Dataset with COMPLETE column definitions from actual Snowflake tables
This reads table structures directly, not via SEMANTIC_VIEW
"""

import sys
import os
import json

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests
import snowflake.connector

def main():
    """Create a Push dataset with complete table definitions from Snowflake."""
    print("="*80)
    print("CREATE COMPLETE PUSH DATASET FROM SNOWFLAKE")
    print("="*80)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        workspace_id = fabric_config.workspace_id
        
        print("Connecting to Snowflake...")
        # Connect directly to Snowflake
        conn = snowflake.connector.connect(
            account=snowflake_config.account,
            user=snowflake_config.user,
            password=snowflake_config.password.get_secret_value(),
            warehouse=snowflake_config.warehouse,
            database=snowflake_config.database,
            schema=snowflake_config.schema_name
        )
        print("[OK] Connected to Snowflake!")
        print()
        
        cursor = conn.cursor()
        
        # Get all tables in the schema
        print("Reading table structures...")
        cursor.execute(f"""
            SELECT DISTINCT table_name 
            FROM information_schema.columns 
            WHERE table_schema = '{snowflake_config.schema_name}'
              AND table_catalog = '{snowflake_config.database}'
              AND table_name NOT LIKE 'SV_%'
              AND table_name NOT LIKE '%VIEW%'
            ORDER BY table_name
        """)
        
        table_names = [row[0] for row in cursor.fetchall()]
        print(f"[OK] Found {len(table_names)} tables")
        print()
        
        # Get complete column definitions for each table
        tables_def = []
        
        for table_name in table_names:
            print(f"Processing table: {table_name}")
            
            # Get all columns for this table
            cursor.execute(f"""
                SELECT 
                    column_name,
                    data_type,
                    is_nullable,
                    ordinal_position
                FROM information_schema.columns
                WHERE table_schema = '{snowflake_config.schema_name}'
                  AND table_catalog = '{snowflake_config.database}'
                  AND table_name = '{table_name}'
                ORDER BY ordinal_position
            """)
            
            columns = cursor.fetchall()
            
            if not columns:
                print(f"  [SKIP] No columns found")
                continue
            
            columns_def = []
            for col_name, data_type, is_nullable, ord_pos in columns:
                # Map Snowflake type to Power BI type
                pb_type = map_datatype(data_type)
                
                columns_def.append({
                    "name": col_name,
                    "dataType": pb_type
                })
            
            tables_def.append({
                "name": table_name,
                "columns": columns_def
            })
            
            print(f"  [OK] {len(columns_def)} columns")
        
        cursor.close()
        conn.close()
        
        print()
        print("="*80)
        print("DATASET SUMMARY")
        print("="*80)
        
        total_columns = sum(len(t["columns"]) for t in tables_def)
        print(f"Tables: {len(tables_def)}")
        print(f"Total Columns: {total_columns}")
        print()
        
        for table in tables_def:
            print(f"  {table['name']}")
            for col in table['columns']:
                print(f"    - {col['name']:30s} ({col['dataType']})")
        print()
        
        # Ask for confirmation
        dataset_name = "SnowflakeComplete"
        print("="*80)
        print(f"Ready to create dataset: {dataset_name}")
        print("="*80)
        response = input("Create this dataset? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
        
        # Authenticate with Fabric
        print()
        print("Authenticating with Fabric...")
        oauth_client = FabricOAuthClient(config=fabric_config)
        token = oauth_client.get_access_token()
        print("[OK] Authenticated!")
        print()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Create dataset definition
        dataset_definition = {
            "name": dataset_name,
            "defaultMode": "Push",
            "tables": tables_def
        }
        
        # Create the dataset
        create_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
        
        print("Creating dataset...")
        print(f"URL: {create_url}")
        print()
        
        response = requests.post(create_url, headers=headers, json=dataset_definition)
        
        if response.status_code in [200, 201]:
            result = response.json()
            new_dataset_id = result.get('id')
            
            print()
            print("="*80)
            print("SUCCESS!")
            print("="*80)
            print(f"Dataset Name: {dataset_name}")
            print(f"Dataset ID: {new_dataset_id}")
            print(f"Tables: {len(tables_def)}")
            print(f"Total Columns: {total_columns}")
            print()
            print("="*80)
            print("UPDATE YOUR .ENV FILE")
            print("="*80)
            print()
            print(f"FABRIC_DATASET_ID={new_dataset_id}")
            print()
            print("Then test with:")
            print("  python -m semantic_sync.main preview --direction snowflake-to-fabric")
            print()
            print("If preview shows 0 changes, you're ready to sync data!")
            print()
            
        elif response.status_code == 409:
            print()
            print("[WARNING] Dataset already exists!")
            print(f"Response: {response.text}")
            
        else:
            print()
            print(f"[ERROR] Failed to create dataset")
            print(f"Status Code: {response.status_code}")
            print(f"Response: {response.text}")
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

def map_datatype(snowflake_type: str) -> str:
    """Map Snowflake data types to Power BI data types."""
    snowflake_type = snowflake_type.upper()
    
    # Numeric types
    if 'NUMBER' in snowflake_type or 'DECIMAL' in snowflake_type or 'NUMERIC' in snowflake_type:
        return 'Double'
    elif snowflake_type == 'INT' or 'INTEGER' in snowflake_type:
        return 'Int64'
    elif 'FLOAT' in snowflake_type or 'DOUBLE' in snowflake_type:
        return 'Double'
    
    # String types  
    elif 'TEXT' in snowflake_type or 'VARCHAR' in snowflake_type or 'CHAR' in snowflake_type or 'STRING' in snowflake_type:
        return 'String'
    
    # Date/Time types
    elif 'TIMESTAMP' in snowflake_type:
        return 'DateTime'
    elif snowflake_type == 'DATE':
        return 'DateTime'
    elif 'TIME' in snowflake_type:
        return 'String'  # Power BI doesn't have a Time type
    
    # Boolean
    elif 'BOOLEAN' in snowflake_type or 'BOOL' in snowflake_type:
        return 'Boolean'
    
    # Default to String for unknown types
    else:
        print(f"    [WARNING] Unknown type '{snowflake_type}' - mapping to String")
        return 'String'

if __name__ == "__main__":
    main()
