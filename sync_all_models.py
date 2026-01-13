#!/usr/bin/env python3
"""Sync ALL Fabric semantic models to Snowflake."""

from semantic_sync.core.fabric_snowflake_semantic_pipeline import (
    FabricToSnowflakePipeline,
    SyncMode,
)

def main():
    # Initialize pipeline
    print("=" * 60)
    print("  BATCH SYNC: All Fabric Models -> Snowflake")
    print("=" * 60)
    
    pipeline = FabricToSnowflakePipeline.from_env()
    
    # List all available models
    print("\nFetching all Fabric models...")
    models = pipeline.list_available_models()
    print(f"Found {len(models)} models:\n")
    for i, m in enumerate(models, 1):
        print(f"  {i}. {m['name']} (ID: {m['id'][:12]}...)")
    
    # Sync ALL models
    print("\n" + "-" * 60)
    print("Syncing ALL models to Snowflake...")
    print("-" * 60)
    
    results = pipeline.sync_all_models(
        mode=SyncMode.METADATA_ONLY,
        dry_run=False
    )
    
    # Summary
    print("\n" + "=" * 60)
    print("  BATCH SYNC SUMMARY")
    print("=" * 60)
    successful = sum(1 for r in results if r.success)
    failed = len(results) - successful
    print(f"  Total Models: {len(results)}")
    print(f"  Successful:   {successful}")
    print(f"  Failed:       {failed}")
    
    if failed > 0:
        print("\n  Failed models:")
        for r in results:
            if not r.success:
                print(f"    - {r.model_name}: {r.error_message}")
    
    print("=" * 60)
    return 0 if failed == 0 else 1

if __name__ == "__main__":
    exit(main())
