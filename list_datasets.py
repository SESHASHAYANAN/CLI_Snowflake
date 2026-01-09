"""
Script to list all datasets in a Power BI/Fabric workspace using the REST API.
This will help you find the correct dataset ID for your semantic model.
"""

import sys
import os

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient

def main():
    """List all datasets in the configured workspace."""
    print("=" * 60)
    print("Power BI / Fabric Dataset Finder")
    print("=" * 60)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        
        print(f"Workspace ID: {fabric_config.workspace_id}")
        print()
        print("Authenticating with Microsoft Fabric...")
        
        # Create OAuth client with FabricConfig object
        oauth_client = FabricOAuthClient(config=fabric_config)
        
        # Get access token
        token = oauth_client.get_access_token()
        print("[OK] Authentication successful!")
        print()
        
        # Call Power BI API to list datasets
        import requests
        
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{fabric_config.workspace_id}/datasets"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        print(f"Fetching datasets from workspace...")
        print(f"API URL: {url}")
        print()
        
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            data = response.json()
            datasets = data.get("value", [])
            
            if not datasets:
                print("[WARNING] No datasets found in this workspace.")
                print()
                print("This could mean:")
                print("  1. The workspace is empty")
                print("  2. You don't have access to view datasets")
                print("  3. The workspace ID is incorrect")
                return
            
            print(f"[OK] Found {len(datasets)} dataset(s) in workspace:")
            print()
            print("-" * 60)
            
            for idx, dataset in enumerate(datasets, 1):
                dataset_id = dataset.get("id", "N/A")
                dataset_name = dataset.get("name", "Unnamed")
                is_refreshable = dataset.get("isRefreshable", False)
                configured_by = dataset.get("configuredBy", "N/A")
                
                print(f"{idx}. {dataset_name}")
                print(f"   Dataset ID: {dataset_id}")
                print(f"   Refreshable: {is_refreshable}")
                print(f"   Configured By: {configured_by}")
                
                # Check if this is the currently configured dataset
                if dataset_id == fabric_config.dataset_id:
                    print(f"   [*] CURRENTLY CONFIGURED IN .env")
                
                print("-" * 60)
            
            print()
            print("TIP: To use a dataset, copy its Dataset ID and update the")
            print("     FABRIC_DATASET_ID value in your .env file")
            
        elif response.status_code == 404:
            print(f"[ERROR] Workspace not found (404)")
            print(f"   The workspace ID '{fabric_config.workspace_id}' does not exist")
            print(f"   or you don't have access to it.")
            print()
            print(f"   Response: {response.text}")
            
        elif response.status_code == 401:
            print(f"[ERROR] Unauthorized (401)")
            print(f"   Your credentials may be invalid or expired.")
            print()
            print(f"   Response: {response.text}")
            
        elif response.status_code == 403:
            print(f"[ERROR] Forbidden (403)")
            print(f"   You don't have permission to access this workspace.")
            print()
            print(f"   Response: {response.text}")
            
        else:
            print(f"[ERROR] API request failed with status code {response.status_code}")
            print(f"   Response: {response.text}")
            sys.exit(1)
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
