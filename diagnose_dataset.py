"""
Diagnostic script to troubleshoot the continent dataset and identify why
we're getting 404 errors when accessing tables.
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

def test_endpoint(url, headers, description):
    """Test an API endpoint and report results."""
    print(f"\n{'='*60}")
    print(f"Testing: {description}")
    print(f"URL: {url}")
    print(f"{'='*60}")
    
    try:
        response = requests.get(url, headers=headers)
        print(f"Status Code: {response.status_code}")
        
        if response.status_code == 200:
            print("[OK] Request successful!")
            try:
                data = response.json()
                print(f"\nResponse (JSON):")
                print(json.dumps(data, indent=2))
                return data
            except:
                print(f"\nResponse (Text):")
                print(response.text[:500])
                return None
        else:
            print(f"[FAIL] Request failed")
            print(f"Response: {response.text}")
            return None
            
    except Exception as e:
        print(f"[ERROR] Exception: {e}")
        return None

def main():
    """Diagnose the continent dataset."""
    print("="*60)
    print("Dataset Diagnostic Tool")
    print("="*60)
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
        print("[OK] Authentication successful!")
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Test 1: Get specific dataset info
        url1 = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}"
        dataset_info = test_endpoint(url1, headers, "Get Dataset Details")
        
        # Test 2: Get dataset tables (the failing endpoint)
        url2 = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
        tables_info = test_endpoint(url2, headers, "Get Dataset Tables")
        
        # Test 3: Get dataset datasources
        url3 = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources"
        datasources_info = test_endpoint(url3, headers, "Get Dataset Datasources")
        
        # Test 4: Get dataset refresh history
        url4 = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
        refresh_info = test_endpoint(url4, headers, "Get Dataset Refresh History")
        
        # Test 5: Try to execute queries endpoint (XMLA)
        url5 = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        print(f"\n{'='*60}")
        print(f"Testing: Execute Queries Endpoint (POST)")
        print(f"URL: {url5}")
        print(f"{'='*60}")
        
        # Simple DAX query to get table list
        query_body = {
            "queries": [
                {
                    "query": "EVALUATE INFO.TABLES()"
                }
            ],
            "serializerSettings": {
                "includeNulls": False
            }
        }
        
        try:
            response = requests.post(url5, headers=headers, json=query_body)
            print(f"Status Code: {response.status_code}")
            
            if response.status_code == 200:
                print("[OK] Query executed successfully!")
                data = response.json()
                print(f"\nQuery Results:")
                print(json.dumps(data, indent=2))
            else:
                print(f"[FAIL] Query failed")
                print(f"Response: {response.text}")
        except Exception as e:
            print(f"[ERROR] Exception: {e}")
        
        # Summary
        print("\n" + "="*60)
        print("DIAGNOSTIC SUMMARY")
        print("="*60)
        
        if dataset_info:
            print(f"[OK] Dataset exists and is accessible")
            if isinstance(dataset_info, dict):
                print(f"     Name: {dataset_info.get('name', 'N/A')}")
                print(f"     Is Refreshable: {dataset_info.get('isRefreshable', 'N/A')}")
        
        if tables_info:
            print(f"[OK] Tables endpoint is working")
            if isinstance(tables_info, dict):
                table_count = len(tables_info.get('value', []))
                print(f"     Found {table_count} table(s)")
        else:
            print(f"[FAIL] Tables endpoint returned 404")
            print(f"       Possible reasons:")
            print(f"       1. Dataset may not support the tables API")
            print(f"       2. Dataset may be a report-only dataset")
            print(f"       3. API permissions may be insufficient")
            print(f"       4. Dataset may need XMLA endpoint enabled")
            
        print("\n" + "="*60)
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
