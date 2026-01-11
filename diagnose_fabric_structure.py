
import sys
import os
import requests
import json
from datetime import datetime

# Add project root
sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient

def main():
    print("="*80)
    print("Fabric Workspace Structure Diagnostics")
    print(f"Time: {datetime.now().isoformat()}")
    print("="*80)
    
    try:
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        workspace_id = fabric_config.workspace_id
        
        print(f"\nWorkspace ID: {workspace_id}")
        
        # Auth
        print("Authenticating...")
        oauth = FabricOAuthClient(fabric_config)
        token = oauth.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        # 1. Get Datasets
        print("\n[1] DATASETS")
        print("-" * 60)
        url_ds = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
        resp_ds = requests.get(url_ds, headers=headers)
        datasets = resp_ds.json().get("value", [])
        
        ds_map = {}
        for ds in datasets:
            ds_id = ds['id']
            ds_name = ds['name']
            owner = ds.get('configuredBy', 'Unknown')
            ds_map[ds_id] = ds_name
            
            print(f"Name: {ds_name}")
            print(f"  ID: {ds_id}")
            print(f"  Owner: {owner}")
            print(f"  Storage: {ds.get('targetStorageMode', 'Unknown')}")
            
            # Probe /tables endpoint
            url_tables = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{ds_id}/tables"
            resp_tables = requests.get(url_tables, headers=headers)
            
            if resp_tables.status_code == 200:
                tables = resp_tables.json().get("value", [])
                print(f"  [ACCESS OK] Found {len(tables)} tables via REST API")
            elif resp_tables.status_code == 404:
                print(f"  [ACCESS FAIL] REST API returned 404 (Not Found) for /tables")
                print(f"                -> This likely indicates an IMPORT mode dataset (PBIX) or XMLA restriction.")
            else:
                print(f"  [ACCESS FAIL] Status {resp_tables.status_code}: {resp_tables.text}")
            print("-" * 60)
            
        # 2. Get Reports
        print("\n\n[2] REPORTS (Validation of Dataset IDs)")
        print("-" * 60)
        url_rep = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/reports"
        resp_rep = requests.get(url_rep, headers=headers)
        reports = resp_rep.json().get("value", [])
        
        for rep in reports:
            rep_name = rep['name']
            rep_ds_id = rep.get('datasetId')
            ds_name = ds_map.get(rep_ds_id, "UNKNOWN DATASET")
            
            print(f"Report: {rep_name}")
            print(f"  Uses Dataset: {ds_name}")
            print(f"  Dataset ID:   {rep_ds_id}")
            
            if ds_name == "UNKNOWN DATASET":
                print("  [WARNING] Linked dataset not found in workspace list!")
            else:
                print("  [OK] Dataset exists.")
            print("-" * 60)

    except Exception as e:
        print(f"\n[ERROR] {e}")

if __name__ == "__main__":
    main()
