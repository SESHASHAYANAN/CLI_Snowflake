import os
import sys
from semantic_sync.config.settings import load_settings
from semantic_sync.core.fabric_client import FabricClient

def main():
    settings = load_settings()
    fabric_config = settings.get_fabric_config()
    client = FabricClient(fabric_config)
    
    print(f"Listing datasets in workspace: {fabric_config.workspace_id}")
    datasets = client.list_workspace_datasets()
    
    print(f"Found {len(datasets)} datasets.")
    for ds in datasets:
        if ds.get("name") == "demo Table":
            print(f"\n[FOUND] demo Table")
            print(f"  ID: {ds.get('id')}")
            print(f"  Name: {ds.get('name')}")
            print(f"  ConfiguredBy: {ds.get('configuredBy')}")
            print(f"  IsRefreshable: {ds.get('isRefreshable')}")
            print(f"  IsEffectiveIdentityRequired: {ds.get('isEffectiveIdentityRequired')}")
            print(f"  TargetStorageMode: {ds.get('targetStorageMode')}")
            print(f"  CreatedDate: {ds.get('createdDate')}")
            # print full json for debug
            # print(ds)

if __name__ == "__main__":
    main()
