"""
Fabric to Snowflake Sync (Batch Mode)

This script syncs ALL semantic models from the configured Fabric/Power BI workspace
to Snowflake using the SemaBridge library (REST API approach).
"""

import sys
import logging
from semantic_sync.config import get_settings
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.core.fabric_model_parser import FabricModelParser
from semantic_sync.core.snowflake_semantic_writer import sync_fabric_to_snowflake
from semantic_sync.utils.logger import setup_logging
from semantic_sync.utils.exceptions import ResourceNotFoundError

def main():
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    print("=" * 70)
    print("[SemaBridge] Fabric Workspace -> Snowflake Sync (Batch)")
    print("=" * 70)
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        print(f"\nSource: Fabric Workspace {fabric_config.workspace_id}")
        print(f"Target: Snowflake {snowflake_config.database}.{snowflake_config.schema_name}")
        
        # 1. List Datasets
        print("\n[1/3] listing datasets in workspace...")
        fabric_client = FabricClient(fabric_config)
        datasets = fabric_client.list_workspace_datasets()
        
        print(f"      Found {len(datasets)} datasets")
        if not datasets:
            print("      No datasets found. Exiting.")
            sys.exit(0)
            
        parser = FabricModelParser(fabric_client, fabric_config)
        
        # Stats
        total = len(datasets)
        success_count = 0
        error_count = 0
        skipped_count = 0
        
        # 2. Iterate and Sync
        print("\n[2/3] Processing datasets...")
        
        for i, ds in enumerate(datasets, 1):
            ds_id = ds.get("id")
            ds_name = ds.get("name", "Unknown")
            is_push = ds.get("addRowsAPIEnabled", False)
            ds_type = "Push" if is_push else "Standard/Import"
            
            print(f"\n   [{i}/{total}] {ds_name} ({ds_type})")
            print(f"   ID: {ds_id}")
            
            try:
                # Read model
                print("      Reading metadata...", end=" ", flush=True)
                model = parser.read_semantic_model(dataset_id=ds_id)
                print(f"[OK] ({len(model.tables)} tables)")
                
                # Sync
                print("      Transmitting to Snowflake...", end=" ", flush=True)
                results = sync_fabric_to_snowflake(
                    fabric_model=model,
                    snowflake_config=snowflake_config,
                    dry_run=False
                )
                
                if results.get("errors", 0) == 0:
                    print("[OK]")
                    success_count += 1
                else:
                    print(f"[WARN] Completed with {results['errors']} errors")
                    success_count += 1 # Count as partial success
                    
            except ResourceNotFoundError as e:
                print(f"[SKIP] Resource not found: {e}")
                skipped_count += 1
            except Exception as e:
                print(f"[ERROR] Failed: {e}")
                error_count += 1
                
        # 3. Summary
        print("\n" + "=" * 70)
        print("BATCH SYNC SUMMARY")
        print("=" * 70)
        print(f"Total Datasets: {total}")
        print(f"Successful:     {success_count}")
        print(f"Failed:         {error_count}")
        print(f"Skipped:        {skipped_count}")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n[CRITICAL ERROR] {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
