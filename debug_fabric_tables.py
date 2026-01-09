
import json
import os
from semantic_sync.config.settings import get_settings
from semantic_sync.core.fabric_client import FabricClient

def main():
    settings = get_settings()
    config = settings.get_fabric_config()
    client = FabricClient(config)
    
    print(f"Dataset ID: {config.dataset_id}")
    dataset = client.get_dataset(config.dataset_id)
    print("Dataset Info:")
    print(json.dumps(dataset, indent=2))
    
    # tables = client.get_dataset_tables(config.dataset_id)
    # print(json.dumps(tables, indent=2))
    
    print("Triggering refresh...")
    try:
        client.trigger_dataset_refresh(config.dataset_id)
        print("Refresh triggered.")
    except Exception as e:
        print(f"Refresh failed: {e}")

if __name__ == "__main__":
    main()
