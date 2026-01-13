"""
Check demo Table using REST API tables endpoint
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

def check_demo_tables():
    settings = load_settings()
    fabric_config = settings.get_fabric_config()
    
    workspace_id = fabric_config.workspace_id
    dataset_id = "d0e5ea6d-f17a-49c0-a331-eda1cb2feeb3"  # demo Table
    
    print(f"Checking demo Table structure")
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
    
    # Try REST API tables endpoint
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
    
    try:
        print("Calling REST API /tables endpoint...")
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            tables = data.get("value", [])
            
            print(f"\n[SUCCESS] Found {len(tables)} tables:")
            print()
            
            for table in tables:
                table_name = table.get("name", "Unknown")
                columns = table.get("columns", [])
                measures = table.get("measures", [])
                
                print(f"Table: {table_name}")
                print(f"  Columns ({len(columns)}):")
                for col in columns:
                    col_name = col.get("name", "?")
                    col_type = col.get("dataType", "?")
                    print(f"    - {col_name} ({col_type})")
                
                if measures:
                    print(f"  Measures ({len(measures)}):")
                    for m in measures:
                        m_name = m.get("name", "?")
                        print(f"    - {m_name}")
                print()
                
        elif response.status_code == 404:
            print("\n[INFO] Tables endpoint returned 404 - Dataset may not support this API")
            print("This can happen with DirectQuery or Import mode datasets")
            
        else:
            print(f"\n[FAIL] Request failed with status {response.status_code}")
            print(f"Response: {response.text}")
            
    except Exception as e:
        print(f"\n[ERROR] Exception: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    check_demo_tables()
