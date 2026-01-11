"""
Auto-Sync Script - Monitors and syncs all datasets

Run this script periodically (or set up as a scheduled task) to:
1. Detect new Push datasets in Fabric
2. Detect new tables in Snowflake
3. Sync all changes automatically
"""

import sys
import os
import json
from datetime import datetime

sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests
import snowflake.connector


def map_snowflake_to_powerbi(sf_type):
    """Map Snowflake data type to Power BI data type."""
    sf_type = sf_type.upper()
    
    if 'NUMBER' in sf_type or 'DECIMAL' in sf_type or 'NUMERIC' in sf_type:
        return 'Double'
    elif sf_type == 'INT' or 'INTEGER' in sf_type:
        return 'Int64'
    elif 'FLOAT' in sf_type or 'DOUBLE' in sf_type:
        return 'Double'
    elif 'TEXT' in sf_type or 'VARCHAR' in sf_type or 'CHAR' in sf_type or 'STRING' in sf_type:
        return 'String'
    elif 'TIMESTAMP' in sf_type:
        return 'DateTime'
    elif sf_type == 'DATE':
        return 'DateTime'
    elif 'BOOLEAN' in sf_type or 'BOOL' in sf_type:
        return 'Boolean'
    else:
        return 'String'


def main():
    print("=" * 70)
    print("AUTO-SYNC: Fabric <-> Snowflake")
    print(f"Time: {datetime.now().isoformat()}")
    print("=" * 70)
    print()

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

    # Connect to Snowflake
    print("2. Connecting to Snowflake...")
    sf_conn = snowflake.connector.connect(
        account=snowflake_config.account,
        user=snowflake_config.user,
        password=snowflake_config.password.get_secret_value(),
        warehouse=snowflake_config.warehouse,
        database=snowflake_config.database,
        schema=snowflake_config.schema_name
    )
    print("   [OK]")

    # Get Snowflake tables
    print("3. Reading Snowflake tables...")
    cursor = sf_conn.cursor()
    schema = snowflake_config.schema_name
    db = snowflake_config.database
    
    cursor.execute(f"""
        SELECT table_name 
        FROM {db}.information_schema.tables 
        WHERE table_schema = '{schema}'
        AND table_type = 'BASE TABLE'
    """)
    sf_tables = [row[0] for row in cursor.fetchall()]
    print(f"   Found {len(sf_tables)} tables")

    # Build table definitions
    tables_def = []
    for table_name in sf_tables:
        cursor.execute(f"""
            SELECT column_name, data_type 
            FROM {db}.information_schema.columns 
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """)
        
        columns = []
        for row in cursor.fetchall():
            col_name, data_type = row
            pb_type = map_snowflake_to_powerbi(data_type)
            columns.append({"name": col_name, "dataType": pb_type})
        
        if columns:
            tables_def.append({"name": table_name, "columns": columns})
            print(f"   - {table_name}: {len(columns)} columns")

    # Get existing Fabric datasets
    print("4. Checking Fabric datasets...")
    datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    response = requests.get(datasets_url, headers=headers)
    existing_datasets = {ds["name"]: ds["id"] for ds in response.json().get("value", [])}
    
    # Find or create SnowflakeSync dataset
    dataset_name = "SnowflakeSync"
    
    if dataset_name in existing_datasets:
        print(f"   Dataset '{dataset_name}' exists, updating tables...")
        dataset_id = existing_datasets[dataset_name]
        
        for table_def in tables_def:
            table_name = table_def["name"]
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}"
            response = requests.put(url, headers=headers, json=table_def)
            if response.status_code in [200, 201]:
                print(f"   [OK] Updated: {table_name}")
            else:
                print(f"   [INFO] {table_name}: {response.status_code}")
    else:
        print(f"   Creating new dataset: {dataset_name}")
        payload = {"name": dataset_name, "defaultMode": "Push", "tables": tables_def}
        response = requests.post(datasets_url, headers=headers, json=payload)
        
        if response.status_code in [200, 201]:
            dataset_id = response.json().get("id")
            print(f"   [OK] Created with ID: {dataset_id}")
        else:
            print(f"   [ERROR] {response.text}")

    # Update semantic view
    print("5. Updating Semantic View...")
    cursor.execute(f"""
        CREATE OR REPLACE VIEW {db}.{schema}.SEMANTIC_VIEW AS
        SELECT table_name, column_name, data_type
        FROM {db}.information_schema.columns
        WHERE table_schema = '{schema}'
        AND table_name NOT LIKE 'SEMANTIC%'
    """)
    print("   [OK]")

    cursor.close()
    sf_conn.close()

    print()
    print("=" * 70)
    print("SYNC COMPLETE!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  - Add new tables to Snowflake and run this script again")
    print("  - Or run: python -m semantic_sync.main sync --direction snowflake-to-fabric")


if __name__ == "__main__":
    main()
