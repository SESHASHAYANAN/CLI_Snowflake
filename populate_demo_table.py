"""
Populate the demo Table dataset in Fabric with sample tables
"""
import sys
import os
import json

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from semantic_sync.config.settings import load_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests

def populate_demo_table():
    settings = load_settings()
    fabric_config = settings.get_fabric_config()
    
    workspace_id = fabric_config.workspace_id
    dataset_id = "d0e5ea6d-f17a-49c0-a331-eda1cb2feeb3"  # demo Table
    
    print(f"Populating demo Table with sample data")
    print(f"Workspace ID: {workspace_id}")
    print(f"Dataset ID: {dataset_id}")
    print()
    
    # Authenticate
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Define sample table schema
    table_payload = {
        "name": "Products",
        "columns": [
            {
                "name": "ProductID",
                "dataType": "int64"
            },
            {
                "name": "ProductName",
                "dataType": "string"
            },
            {
                "name": "Category",
                "dataType": "string"
            },
            {
                "name": "Price",
                "dataType": "double"
            }
        ]
    }
    
    # Sample rows
    rows = [
        {
            "ProductID": 1,
            "ProductName": "Super Widget",
            "Category": "Widgets",
            "Price": 19.99
        },
        {
            "ProductID": 2,
            "ProductName": "Mega Gadget",
            "Category": "Gadgets",
            "Price": 49.99
        },
        {
            "ProductID": 3,
            "ProductName": "Ultra Thingy",
            "Category": "Widgets",
            "Price": 29.50
        }
    ]
    
    try:
        # Step 1: Create table (PUT to add/update table)
        print("Step 1: Creating 'Products' table...")
        create_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables/Products"
        
        response = requests.put(create_url, headers=headers, json=table_payload)
        
        if response.status_code in [200, 201]:
            print(f"✅ Table 'Products' created successfully")
        else:
            print(f"⚠️  Failed to create table: {response.status_code}")
            print(f"Response: {response.text}")
            return
        
        # Step 2: Add rows to table
        print("\nStep 2: Adding sample data to 'Products' table...")
        rows_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables/Products/rows"
        
        rows_payload = {"rows": rows}
        response = requests.post(rows_url, headers=headers, json=rows_payload)
        
        if response.status_code in [200, 201]:
            print(f"✅ Added {len(rows)} rows to 'Products' table")
        else:
            print(f"⚠️  Failed to add rows: {response.status_code}")
            print(f"Response: {response.text}")
            return
            
        print("\n" + "="*60)
        print("✅ SUCCESS: demo Table populated with sample data!")
        print("="*60)
        print("\nNext steps:")
        print("1. Sync to Snowflake:")
        print("   semantic-sync sync fabric-to-snowflake")
        print("\n2. Verify sync:")
        print("   python check_demo_table.py")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    populate_demo_table()
