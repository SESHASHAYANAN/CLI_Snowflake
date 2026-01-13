"""
Create a new demo Table as a Push API dataset with sample data
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

def create_demo_push_dataset():
    settings = load_settings()
    fabric_config = settings.get_fabric_config()
    
    workspace_id = fabric_config.workspace_id
    
    print(f"Creating demo Table as Push API dataset")
    print(f"Workspace ID: {workspace_id}")
    print()
    
    # Authenticate
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Define dataset with sample table
    dataset_payload = {
        "name": "demo Table Push",
        "defaultMode": "Push",
        "tables": [
            {
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
        ]
    }
    
    try:
        # Create Push API dataset
        print("Creating Push API dataset...")
        create_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
        
        response = requests.post(create_url, headers=headers, json=dataset_payload)
        
        if response.status_code in [200, 201]:
            dataset = response.json()
            dataset_id = dataset.get("id")
            print(f"✅ Dataset created successfully")
            print(f"   Dataset ID: {dataset_id}")
            print(f"   Dataset Name: {dataset.get('name')}")
            
            # Add sample rows
            print("\nAdding sample data...")
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
            
            rows_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables/Products/rows"
            rows_payload = {"rows": rows}
            response = requests.post(rows_url, headers=headers, json=rows_payload)
            
            if response.status_code in [200, 201]:
                print(f"✅ Added {len(rows)} rows to 'Products' table")
            else:
                print(f"⚠️  Failed to add rows: {response.status_code}")
                print(f"Response: {response.text}")
            
            print("\n" + "="*60)
            print("✅ SUCCESS: demo Table Push dataset created!")
            print("="*60)
            print(f"\nDataset ID: {dataset_id}")
            print("\nNext steps:")
            print("1. Sync to Snowflake:")
            print("   semantic-sync sync fabric-to-snowflake")
            print("\n2. Verify sync:")
            print(f"   Update check_demo_table.py to use model 'demo Table Push'")
            
        else:
            print(f"❌ Failed to create dataset: {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    create_demo_push_dataset()
