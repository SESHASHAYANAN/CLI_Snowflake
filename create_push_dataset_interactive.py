"""
Interactive Push Dataset Creator

Use this to create a new syncable dataset in Fabric.
The dataset will be created as a Push dataset (syncable with Snowflake).
"""

import sys
import os

sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests
import snowflake.connector


def map_type_to_powerbi(user_type):
    """Map user-friendly type names to Power BI types."""
    type_map = {
        "text": "String",
        "string": "String",
        "varchar": "String",
        "number": "Double",
        "int": "Int64",
        "integer": "Int64",
        "decimal": "Double",
        "float": "Double",
        "double": "Double",
        "date": "DateTime",
        "datetime": "DateTime",
        "timestamp": "DateTime",
        "bool": "Boolean",
        "boolean": "Boolean",
    }
    return type_map.get(user_type.lower(), "String")


def map_type_to_snowflake(pb_type):
    """Map Power BI types to Snowflake types."""
    type_map = {
        "String": "VARCHAR(500)",
        "Int64": "INTEGER",
        "Double": "FLOAT",
        "DateTime": "TIMESTAMP",
        "Boolean": "BOOLEAN",
    }
    return type_map.get(pb_type, "VARCHAR(500)")


def main():
    print("=" * 70)
    print("CREATE NEW SYNCABLE DATASET")
    print("=" * 70)
    print()
    print("This will create a new Push dataset in Fabric AND a matching")
    print("table in Snowflake, so they stay in sync.")
    print()

    # Get dataset name
    dataset_name = input("Enter dataset/table name: ").strip()
    if not dataset_name:
        print("Name is required!")
        return

    # Get columns
    print()
    print("Now define columns. Enter each column as: name,type")
    print("Types: text, number, int, decimal, date, bool")
    print("Enter blank line when done.")
    print()

    columns = []
    while True:
        col_input = input(f"Column {len(columns) + 1}: ").strip()
        if not col_input:
            break
        
        parts = col_input.split(",")
        if len(parts) != 2:
            print("  Invalid format. Use: name,type")
            continue
        
        col_name = parts[0].strip().upper()
        col_type = map_type_to_powerbi(parts[1].strip())
        
        columns.append({"name": col_name, "dataType": col_type})
        print(f"  Added: {col_name} ({col_type})")

    if not columns:
        print("At least one column is required!")
        return

    print()
    print(f"Creating dataset '{dataset_name}' with {len(columns)} columns...")
    print()

    # Connect to services
    settings = get_settings()
    fabric_config = settings.get_fabric_config()
    snowflake_config = settings.get_snowflake_config()
    workspace_id = fabric_config.workspace_id

    # Authenticate Fabric
    print("1. Authenticating with Fabric...")
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    print("   [OK]")

    # Create Fabric dataset
    print("2. Creating Fabric Push dataset...")
    datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    
    payload = {
        "name": dataset_name,
        "defaultMode": "Push",
        "tables": [{
            "name": dataset_name,
            "columns": columns
        }]
    }
    
    response = requests.post(datasets_url, headers=headers, json=payload)
    
    if response.status_code in [200, 201]:
        result = response.json()
        dataset_id = result.get("id")
        print(f"   [OK] Created with ID: {dataset_id}")
    elif response.status_code == 409:
        print("   [ERROR] Dataset already exists with this name!")
        return
    else:
        print(f"   [ERROR] {response.text}")
        return

    # Create Snowflake table
    print("3. Creating Snowflake table...")
    sf_conn = snowflake.connector.connect(
        account=snowflake_config.account,
        user=snowflake_config.user,
        password=snowflake_config.password.get_secret_value(),
        warehouse=snowflake_config.warehouse,
        database=snowflake_config.database,
        schema=snowflake_config.schema_name
    )
    cursor = sf_conn.cursor()
    
    col_defs = []
    for col in columns:
        sf_type = map_type_to_snowflake(col["dataType"])
        col_defs.append(f'"{col["name"]}" {sf_type}')
    
    schema = snowflake_config.schema_name
    db = snowflake_config.database
    
    sql = f'CREATE TABLE IF NOT EXISTS {db}.{schema}."{dataset_name}" ({", ".join(col_defs)})'
    
    try:
        cursor.execute(sql)
        print("   [OK] Snowflake table created")
    except Exception as e:
        print(f"   [ERROR] {e}")
        return
    finally:
        cursor.close()
        sf_conn.close()

    print()
    print("=" * 70)
    print("SUCCESS!")
    print("=" * 70)
    print()
    print(f"Dataset Name: {dataset_name}")
    print(f"Fabric ID:    {dataset_id}")
    print(f"Columns:      {len(columns)}")
    print()
    print("Both Fabric and Snowflake now have matching structures.")
    print("Run 'python auto_sync.py' anytime to sync changes.")
    print()


if __name__ == "__main__":
    main()
