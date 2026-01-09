"""
Comprehensive Fabric Workspace Explorer
Shows all datasets with full details
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
    """Explore all datasets in Fabric workspace."""
    print("="*80)
    print("FABRIC WORKSPACE - COMPLETE DATASET INVENTORY")
    print("="*80)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        
        workspace_id = fabric_config.workspace_id
        
        # Authenticate
        print("Authenticating with Microsoft Fabric...")
        oauth_client = FabricOAuthClient(config=fabric_config)
        token = oauth_client.get_access_token()
        print("[OK] Authentication successful!")
        print()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Get workspace info
        print("="*80)
        print("WORKSPACE INFORMATION")
        print("="*80)
        workspace_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}"
        response = requests.get(workspace_url, headers=headers)
        
        if response.status_code == 200:
            workspace_data = response.json()
            print(f"Name: {workspace_data.get('name', 'Unknown')}")
            print(f"ID: {workspace_id}")
            print(f"Type: {workspace_data.get('type', 'Unknown')}")
            print(f"State: {workspace_data.get('state', 'Unknown')}")
            print(f"Is On Dedicated Capacity: {workspace_data.get('isOnDedicatedCapacity', False)}")
            print(f"Capacity ID: {workspace_data.get('capacityId', 'None')}")
        print()
        
        # Get all datasets
        print("="*80)
        print("ALL DATASETS IN WORKSPACE")
        print("="*80)
        datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
        response = requests.get(datasets_url, headers=headers)
        
        if response.status_code != 200:
            print(f"[ERROR] Failed to fetch datasets: {response.text}")
            return
        
        datasets = response.json().get("value", [])
        print(f"Total Datasets Found: {len(datasets)}")
        print()
        
        for idx, dataset in enumerate(datasets, 1):
            print("="*80)
            print(f"DATASET #{idx}: {dataset.get('name', 'Unnamed')}")
            print("="*80)
            
            # Basic Info
            print(f"ID: {dataset.get('id')}")
            print(f"Name: {dataset.get('name')}")
            print(f"Configured By: {dataset.get('configuredBy', 'N/A')}")
            print(f"Created: {dataset.get('createdDate', 'N/A')}")
            
            # Type Info
            print(f"\nDataset Type:")
            print(f"  - Is Push Dataset: {dataset.get('addRowsAPIEnabled', False)}")
            print(f"  - Is Refreshable: {dataset.get('isRefreshable', False)}")
            print(f"  - Target Storage Mode: {dataset.get('targetStorageMode', 'N/A')}")
            print(f"  - Is On-Prem Gateway Required: {dataset.get('isOnPremGatewayRequired', False)}")
            
            # Capabilities
            print(f"\nCapabilities:")
            print(f"  - Effective Identity Required: {dataset.get('isEffectiveIdentityRequired', False)}")
            print(f"  - Effective Identity Roles Required: {dataset.get('isEffectiveIdentityRolesRequired', False)}")
            
            # Web URL
            print(f"\nWeb URL: {dataset.get('webUrl', 'N/A')}")
            
            # Try to get datasources
            dataset_id = dataset.get('id')
            datasources_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/datasources"
            ds_response = requests.get(datasources_url, headers=headers)
            
            if ds_response.status_code == 200:
                datasources = ds_response.json().get("value", [])
                if datasources:
                    print(f"\nData Sources ({len(datasources)}):")
                    for ds_idx, ds in enumerate(datasources, 1):
                        print(f"  {ds_idx}. Type: {ds.get('datasourceType', 'Unknown')}")
                        conn_details = ds.get('connectionDetails', {})
                        if 'url' in conn_details:
                            print(f"     URL: {conn_details.get('url')}")
                        if 'server' in conn_details:
                            print(f"     Server: {conn_details.get('server')}")
                        if 'database' in conn_details:
                            print(f"     Database: {conn_details.get('database')}")
            
            # Try to get refresh history
            refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/refreshes?$top=1"
            refresh_response = requests.get(refresh_url, headers=headers)
            
            if refresh_response.status_code == 200:
                refreshes = refresh_response.json().get("value", [])
                if refreshes:
                    last_refresh = refreshes[0]
                    print(f"\nLast Refresh:")
                    print(f"  - Status: {last_refresh.get('status', 'Unknown')}")
                    print(f"  - Type: {last_refresh.get('refreshType', 'Unknown')}")
                    print(f"  - Start: {last_refresh.get('startTime', 'N/A')}")
                    print(f"  - End: {last_refresh.get('endTime', 'N/A')}")
            
            # Check if it's the configured dataset
            if dataset_id == fabric_config.dataset_id:
                print(f"\n>>> THIS IS YOUR CONFIGURED DATASET IN .env <<<")
            
            # Check compatibility with semantic-sync
            is_push = dataset.get('addRowsAPIEnabled', False)
            print(f"\nSemantic-Sync Compatibility:")
            if is_push:
                print(f"  [OK] COMPATIBLE - This is a Push dataset")
                print(f"       Can use REST API /tables endpoint")
                print(f"       Supports schema modifications via API")
            else:
                print(f"  [LIMITED] This is an Import dataset")
                print(f"       Cannot use REST API /tables endpoint")
                print(f"       Cannot modify schema via REST API")
                print(f"       Would need XMLA/Premium for full support")
            
            print()
        
        # Summary
        print("="*80)
        print("SUMMARY")
        print("="*80)
        push_count = sum(1 for d in datasets if d.get('addRowsAPIEnabled', False))
        import_count = len(datasets) - push_count
        
        print(f"Total Datasets: {len(datasets)}")
        print(f"  - Push API Datasets: {push_count}")
        print(f"  - Import Datasets: {import_count}")
        print()
        
        if push_count > 0:
            print("[OK] You have Push datasets that can work with semantic-sync!")
            print("\nPush Datasets:")
            for dataset in datasets:
                if dataset.get('addRowsAPIEnabled', False):
                    print(f"  - {dataset.get('name')} (ID: {dataset.get('id')})")
        else:
            print("[INFO] No Push API datasets found.")
            print("       All your datasets are Import datasets.")
            print("\nTo enable Snowflake->Fabric sync, you need to either:")
            print("  1. Create a new Push dataset")
            print("  2. Upgrade to Premium/Fabric capacity for XMLA support")
        
        print()
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
