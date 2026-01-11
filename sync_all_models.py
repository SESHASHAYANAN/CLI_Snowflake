
import sys
import logging
import requests
from copy import deepcopy
from datetime import datetime

# Add project root to path
import os
sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.core import FabricClient, SemanticUpdater, SyncDirection, SyncMode
from semantic_sync.auth.oauth import FabricOAuthClient

def main():
    # Setup logging
    logging.basicConfig(level=logging.WARNING) # Reduce noise
    logger = logging.getLogger("sync_all_models")
    
    print("="*70)
    print("SemaBridge: Batch Sync All Fabric Models to Snowflake")
    print(f"Time: {datetime.now().isoformat()}")
    print("="*70)
    
    try:
        settings = get_settings()
        base_fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        # 1. List Datasets
        print("\n1. Discovering Fabric Datasets...")
        
        oauth = FabricOAuthClient(base_fabric_config)
        token = oauth.get_access_token()
        headers = {"Authorization": f"Bearer {token}"}
        
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{base_fabric_config.workspace_id}/datasets"
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        datasets = resp.json().get("value", [])
        
        print(f"   Found {len(datasets)} datasets in workspace.")
        
        # 2. Iterate and Sync
        print("\n2. Starting Batch Synchronization...")
        
        success_count = 0
        fail_count = 0
        
        for i, ds in enumerate(datasets, 1):
            ds_name = ds['name']
            ds_id = ds['id']
            
            print(f"\n   [{i}/{len(datasets)}] Syncing Model: {ds_name}")
            print(f"         ID: {ds_id}")
            
            # Create isolated config for this dataset
            current_fabric_config = deepcopy(base_fabric_config)
            current_fabric_config.dataset_id = ds_id
            
            try:
                # Initialize Updater
                updater = SemanticUpdater(
                    fabric_config=current_fabric_config,
                    snowflake_config=snowflake_config
                )
                
                # Sync Fabric -> Snowflake (Metadata Only)
                # We use METADATA_ONLY to be safe and fast for batch operations
                result = updater.sync(
                    direction=SyncDirection.FABRIC_TO_SNOWFLAKE,
                    mode=SyncMode.METADATA_ONLY,
                    dry_run=False
                )
                
                if result.success:
                    print(f"         [OK] Success")
                    print(f"         Details: {result.changes_applied} changes applied, {result.errors} errors.")
                    success_count += 1
                else:
                    print(f"         [FAIL] Failed: {result.error_message}")
                    fail_count += 1
                    
            except Exception as e:
                print(f"         [ERROR] Unexpected error: {e}")
                fail_count += 1
                
        print("\n" + "="*70)
        print(f"BATCH SYNC COMPLETE")
        print(f"Successful: {success_count}")
        print(f"Failed:     {fail_count}")
        print("="*70)

    except Exception as e:
        print(f"\n[FATAL ERROR] Script failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
