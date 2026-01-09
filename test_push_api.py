"""
Test Power BI Push API - Diagnose table creation issues
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
    """Test Power BI Push API endpoints."""
    print("="*80)
    print("POWER BI PUSH API - DIAGNOSTIC TEST")
    print("="*80)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        
        workspace_id = fabric_config.workspace_id
        dataset_id = fabric_config.dataset_id
        
        print(f"Workspace ID: {workspace_id}")
        print(f"Dataset ID: {dataset_id}")
        print()
        
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
        
        # Test 1: Get dataset info
        print("="*80)
        print("TEST 1: Get Dataset Information")
        print("="*80)
        dataset_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"
        response = requests.get(dataset_url, headers=headers)
        
        if response.status_code == 200:
            dataset = response.json()
            print(f"[OK] Dataset found: {dataset.get('name')}")
            print(f"     Type: Push = {dataset.get('addRowsAPIEnabled', False)}")
            print(f"     Is Refreshable: {dataset.get('isRefreshable', False)}")
        else:
            print(f"[ERROR] {response.status_code}: {response.text}")
        print()
        
        # Test 2: Try to get existing tables
        print("="*80)
        print("TEST 2: Get Existing Tables")
        print("="*80)
        tables_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
        response = requests.get(tables_url, headers=headers)
        
        if response.status_code == 200:
            tables_data = response.json()
            tables = tables_data.get('value', [])
            print(f"[OK] Found {len(tables)} existing table(s):")
            for table in tables:
                print(f"     - {table.get('name')}")
        else:
            print(f"[ERROR] {response.status_code}: {response.text}")
        print()
        
        # Test 3: Try to create a simple test table using PUT to specific table endpoint
        print("="*80)
        print("TEST 3: Create Test Table (Method 1: PUT /tables/{tableName})")
        print("="*80)
        
        test_table_name = "TestTable1"
        table_def = {
            "name": test_table_name,
            "columns": [
                {
                    "name": "ID",
                    "dataType": "Int64"
                },
                {
                    "name": "Name",
                    "dataType": "String"
                },
                {
                    "name": "Value",
                    "dataType": "Double"
                }
            ]
        }
        
        # Method 1: PUT to /tables/{tableName}
        put_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables/{test_table_name}"
        print(f"URL: {put_url}")
        print(f"Payload:")
        print(json.dumps(table_def, indent=2))
        print()
        
        response = requests.put(put_url, headers=headers, json=table_def)
        print(f"Response Status: {response.status_code}")
        if response.status_code in [200, 201]:
            print("[OK] Table created successfully!")
            print(f"Response: {response.text}")
        else:
            print(f"[ERROR] Failed: {response.text}")
        print()
        
        # Test 4: Try POST to /tables collection
        print("="*80)
        print("TEST 4: Create Test Table (Method 2: POST /tables)")
        print("="*80)
        
        test_table_name2 = "TestTable2"
        table_def2 = {
            "name": test_table_name2,
            "columns": [
                {
                    "name": "ID",
                    "dataType": "Int64"
                },
                {
                    "name": "Description",
                    "dataType": "String"
                }
            ]
        }
        
        post_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
        print(f"URL: {post_url}")
        print(f"Payload:")
        print(json.dumps(table_def2, indent=2))
        print()
        
        response = requests.post(post_url, headers=headers, json=table_def2)
        print(f"Response Status: {response.status_code}")
        if response.status_code in [200, 201]:
            print("[OK] Table created successfully!")
            print(f"Response: {response.text}")
        else:
            print(f"[ERROR] Failed: {response.text}")
        print()
        
        # Test 5: Check tables again
        print("="*80)
        print("TEST 5: List Tables After Creation Attempts")
        print("="*80)
        response = requests.get(tables_url, headers=headers)
        
        if response.status_code == 200:
            tables_data = response.json()
            tables = tables_data.get('value', [])
            print(f"[OK] Found {len(tables)} table(s):")
            for table in tables:
                print(f"     - {table.get('name')} ({len(table.get('columns', []))} columns)")
        else:
            print(f"[ERROR] {response.status_code}: {response.text}")
        print()
        
        # Summary
        print("="*80)
        print("DIAGNOSTIC SUMMARY")
        print("="*80)
        print()
        print("Based on the results above:")
        print("1. If Method 1 (PUT) worked → Use PUT /tables/{tableName}")
        print("2. If Method 2 (POST) worked → Use POST /tables")
        print("3. If neither worked → Check permissions or dataset type")
        print()
        print("Common issues:")
        print("- Dataset must be a Push dataset (addRowsAPIEnabled=true)")
        print("- Service principal needs Dataset.ReadWrite.All permission")
        print("- Workspace must allow API access")
        print()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
