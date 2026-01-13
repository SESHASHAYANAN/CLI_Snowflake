#!/usr/bin/env python3
"""Test sync for continent model specifically."""

from semantic_sync.core.fabric_snowflake_semantic_pipeline import FabricToSnowflakePipeline, SyncMode

def main():
    pipeline = FabricToSnowflakePipeline.from_env()
    models = pipeline.list_available_models()

    # Find and sync the 'continent' model
    continent = next((m for m in models if m['name'] == 'continent'), None)
    if continent:
        print(f"Syncing continent model (ID: {continent['id']})")
        result = pipeline.sync_semantic_model(model_id=continent['id'], mode=SyncMode.METADATA_ONLY, dry_run=False)
        print(f"Result: Tables={result.tables_synced}, Columns={result.columns_synced}")
        return result.success
    else:
        print('Could not find continent model')
        return False

if __name__ == "__main__":
    main()
