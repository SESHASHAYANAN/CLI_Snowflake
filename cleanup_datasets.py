"""
Cleanup datasets in Fabric workspace.
"""
import sys
import os
import requests
from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient

def main():
    settings = get_settings()
    fabric_config = settings.get_fabric_config()
    workspace_id = fabric_config.workspace_id
    
    print(f"Authenticating for workspace: {workspace_id}")
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    # List datasets
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    datasets = response.json().get('value', [])
    
    print(f"Found {len(datasets)} datasets.")
    
    for ds in datasets:
        name = ds.get('name', '')
        ds_id = ds.get('id')
        
        # Cleanup criteria: Name starts with SnowflakeSync, or is 'SalesAnalytics' (to refresh)
        if name.startswith("SnowflakeSync") or name == "SalesAnalytics":
            print(f"Deleting dataset: {name} ({ds_id})")
            del_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{ds_id}"
            del_resp = requests.delete(del_url, headers=headers)
            if del_resp.status_code == 200:
                print("  [OK] Deleted.")
            else:
                print(f"  [FAIL] Failed to delete: {del_resp.status_code}")

if __name__ == "__main__":
    main()
