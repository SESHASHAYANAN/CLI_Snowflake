"""
Create a Push Dataset with ALL Snowflake tables pre-defined
This works around the API limitation that prevents creating new tables after dataset creation
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

def main():
    """Create a Push dataset with all Snowflake tables predefined."""
    print("="*80)
    print("CREATE PUSH DATASET WITH ALL SNOWFLAKE TABLES")
    print("="*80)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        workspace_id = fabric_config.workspace_id
        
        # Authenticate
        print("Authenticating...")
        oauth_client = FabricOAuthClient(config=fabric_config)
        token = oauth_client.get_access_token()
        print("[OK] Authenticated!")
        print()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Read Snowflake schema
        print("Reading Snowflake schema...")
        from semantic_sync.core.snowflake_reader import SnowflakeReader
        
        reader = SnowflakeReader(snowflake_config)
        semantic_model = reader.read_semantic_view()
        
        print(f"[OK] Found {len(semantic_model.tables)} tables in Snowflake")
        print()
        
        # Build dataset definition with ALL tables
        dataset_name = "SnowflakePushDataset"
        
        tables_def = []
        for table in semantic_model.tables:
            columns_def = []
            for column in table.columns:
                # Map Snowflake types to Power BI types
                pb_type = map_datatype(column.data_type)
                columns_def.append({
                    "name": column.name,
                    "dataType": pb_type
                })
            
            if columns_def:  # Only add tables with columns
                tables_def.append({
                    "name": table.name,
                    "columns": columns_def
                })
        
        dataset_definition = {
            "name": dataset_name,
            "defaultMode": "Push",
            "tables": tables_def
        }
        
        # Display what we're creating
        print("="*80)
        print(f"Dataset Definition: {dataset_name}")
        print("="*80)
        print(f"Tables to create: {len(tables_def)}")
        for table in tables_def:
            print(f"  - {table['name']} ({len(table['columns'])} columns)")
        print()
        
        # Ask for confirmation
        response = input("Create this dataset? (y/n): ")
        if response.lower() != 'y':
            print("Cancelled.")
            return
        
        # Create the dataset
        create_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
        
        print()
        print("Creating dataset...")
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
            print()
            print("Tables created:")
            for table in tables_def:
                print(f"  - {table['name']}")
            print()
            print("="*80)
            print("NEXT STEP: Update your .env file")
            print("="*80)
            print()
            print(f"FABRIC_DATASET_ID={new_dataset_id}")
            print()
            print("Then you can sync data (but NOT add new tables!):")
            print("  python -m semantic_sync.main sync --direction snowflake-to-fabric")
            print()
            
        elif response.status_code == 409:
            print()
            print("[WARNING] Dataset already exists!")
            print(f"Response: {response.text}")
            print()
            print("Options:")
            print("1. Delete the existing dataset in Power BI")
            print("2. Use a different name")
            print("3. Use the existing dataset if it has all the tables you need")
            
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
    if snowflake_type in ['NUMBER', 'DECIMAL', 'NUMERIC']:
        return 'Double'
    elif snowflake_type == 'INT':
        return 'Int64'
    elif snowflake_type == 'FLOAT':
        return 'Double'
    
    # String types
    elif 'TEXT' in snowflake_type or 'VARCHAR' in snowflake_type or 'CHAR' in snowflake_type:
        return 'String'
    
    # Date/Time types
    elif 'TIMESTAMP' in snowflake_type:
        return 'DateTime'
    elif snowflake_type == 'DATE':
        return 'DateTime'
    elif snowflake_type == 'TIME':
        return 'String'  # Power BI doesn't have a Time type
    
    # Boolean
    elif snowflake_type == 'BOOLEAN':
        return 'Boolean'
    
    # Default to String for unknown types
    else:
        return 'String'

if __name__ == "__main__":
    main()
