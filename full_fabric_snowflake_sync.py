#!/usr/bin/env python3
"""
Complete Fabric to Snowflake Semantic Sync with Auto-Metadata.

This script:
1. Connects to Microsoft Fabric workspace
2. Lists all semantic models
3. Extracts metadata using multiple fallback strategies:
   - REST API (Push datasets)
   - DMV queries (XMLA access)
   - INFO.TABLES() DAX
   - OneLake (Lakehouse)
   - Auto-metadata (pre-defined definitions)
4. Syncs all metadata to Snowflake
5. Shows comprehensive results

Usage:
    python full_fabric_snowflake_sync.py [--dry-run]
"""

import sys
from datetime import datetime

from semantic_sync.core.fabric_snowflake_semantic_pipeline import (
    FabricToSnowflakePipeline,
    SyncMode,
    SyncResult,
)


def print_banner():
    """Print sync banner."""
    print("=" * 70)
    print("  FABRIC -> SNOWFLAKE SEMANTIC SYNC")
    print("  Complete metadata sync with auto-detection")
    print("=" * 70)
    print(f"  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


def print_model_list(models: list[dict]) -> None:
    """Print list of discovered models."""
    print(f"\n[DISCOVERY] Found {len(models)} semantic models in Fabric:")
    print("-" * 60)
    for i, m in enumerate(models, 1):
        print(f"  {i:2}. {m['name'][:40]:<40} (ID: {m['id'][:12]}...)")
    print("-" * 60)


def print_sync_result(result: SyncResult, index: int, total: int) -> None:
    """Print individual sync result."""
    status = "[OK]" if result.success else "[FAIL]"
    print(f"\n  [{index}/{total}] {result.model_name}")
    print(f"       Status: {status}")
    print(f"       Tables: {result.tables_synced}")
    print(f"       Columns: {result.columns_synced}")
    print(f"       Duration: {result.duration_seconds:.2f}s")
    if not result.success:
        print(f"       Error: {result.error_message}")


def print_summary(results: list[SyncResult]) -> None:
    """Print sync summary."""
    total = len(results)
    successful = sum(1 for r in results if r.success)
    failed = total - successful
    total_tables = sum(r.tables_synced for r in results)
    total_columns = sum(r.columns_synced for r in results)
    total_time = sum(r.duration_seconds for r in results)
    
    print("\n" + "=" * 70)
    print("  SYNC SUMMARY")
    print("=" * 70)
    print(f"  Models Processed:  {total}")
    print(f"  Successful:        {successful}")
    print(f"  Failed:            {failed}")
    print("-" * 70)
    print(f"  Total Tables:      {total_tables}")
    print(f"  Total Columns:     {total_columns}")
    print(f"  Total Duration:    {total_time:.2f}s")
    print("=" * 70)
    
    # List models with their table counts
    print("\n  Model Details:")
    print("-" * 70)
    for r in sorted(results, key=lambda x: x.tables_synced, reverse=True):
        status = "[OK]" if r.success else "[X]"
        print(f"    {status} {r.model_name[:35]:<35} | Tables: {r.tables_synced:2} | Cols: {r.columns_synced:3}")
    print("-" * 70)


def main(dry_run: bool = False) -> int:
    """Run the full sync."""
    print_banner()
    
    # Initialize pipeline
    print("\n[INIT] Initializing Fabric -> Snowflake pipeline...")
    try:
        pipeline = FabricToSnowflakePipeline.from_env()
        print("       [OK] Pipeline initialized")
    except Exception as e:
        print(f"       [FAIL] Pipeline initialization failed: {e}")
        return 1
    
    # List models
    print("\n[DISCOVER] Listing Fabric semantic models...")
    try:
        models = pipeline.list_available_models()
        print_model_list(models)
    except Exception as e:
        print(f"       [FAIL] Failed to list models: {e}")
        return 1
    
    # Sync all models
    mode_str = "[DRY RUN]" if dry_run else "[LIVE]"
    print(f"\n[SYNC] {mode_str} Syncing all models to Snowflake...")
    print("       Using auto-metadata fallback for models without DAX access")
    print("-" * 60)
    
    results: list[SyncResult] = []
    
    for i, model in enumerate(models, 1):
        try:
            result = pipeline.sync_semantic_model(
                model_id=model["id"],
                mode=SyncMode.METADATA_ONLY,
                dry_run=dry_run,
            )
            results.append(result)
            print_sync_result(result, i, len(models))
        except Exception as e:
            # Print error but continue with other models  
            print(f"\n  [{i}/{len(models)}] {model['name']}")
            print(f"       Status: [FAIL]")
            print(f"       Error: {e}")
    
    # Print summary
    print_summary(results)
    
    # Verify in Snowflake
    if not dry_run:
        print("\n[VERIFY] Checking Snowflake for synced metadata...")
        try:
            import snowflake.connector
            import os
            from dotenv import load_dotenv
            
            load_dotenv()
            conn = snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MODEL_NAME, TABLE_COUNT, UPDATED_AT "
                "FROM SEMANTIC_LAYER._SEMANTIC_METADATA "
                "ORDER BY UPDATED_AT DESC"
            )
            rows = cursor.fetchall()
            
            print("\n  Snowflake _SEMANTIC_METADATA table:")
            print("-" * 70)
            for row in rows:
                print(f"    {row[0]:<40} | Tables: {row[1]:2} | Updated: {row[2]}")
            print("-" * 70)
            
            cursor.execute("SELECT COUNT(*) FROM SEMANTIC_LAYER._SEMANTIC_METADATA")
            total = cursor.fetchone()[0]
            print(f"\n  Total models in Snowflake: {total}")
            
            conn.close()
        except Exception as e:
            print(f"       [WARN] Could not verify Snowflake data: {e}")
    
    print("\n" + "=" * 70)
    print("  SYNC COMPLETE")
    print("=" * 70)
    
    return 0 if all(r.success for r in results) else 1


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    sys.exit(main(dry_run=dry_run))
