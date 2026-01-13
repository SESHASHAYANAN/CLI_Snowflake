#!/usr/bin/env python3
"""
Demo: Fabric to Snowflake Semantic Data Sync

This script demonstrates the semantic data synchronization workflow
from Microsoft Fabric to Snowflake.

Usage:
    # Dry run (preview changes without applying)
    python demo_fabric_to_snowflake.py --dry-run
    
    # Actual sync
    python demo_fabric_to_snowflake.py
    
    # Sync specific model
    python demo_fabric_to_snowflake.py --model "SalesAnalytics"
    
    # Full sync mode
    python demo_fabric_to_snowflake.py --mode full

Requirements:
    - Configure .env file with Fabric and Snowflake credentials
    - Install dependencies: pip install -e .
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

# Setup path for development
sys.path.insert(0, ".")

from semantic_sync.core.fabric_snowflake_semantic_pipeline import (
    FabricToSnowflakePipeline,
    SyncMode,
    sync_fabric_to_snowflake,
)
from semantic_sync.utils.logger import setup_logging


# =============================================================================
# Demo Banner
# =============================================================================

DEMO_BANNER = r"""
+===============================================================================+
|                                                                               |
|   FABRIC TO SNOWFLAKE SEMANTIC SYNC DEMO                                      |
|                                                                               |
|   This demo shows how to sync semantic models from Microsoft Fabric           |
|   to Snowflake while preserving all metadata (descriptions, types,            |
|   relationships, measures).                                                   |
|                                                                               |
+===============================================================================+
"""


def print_step(step_num: int, total: int, message: str):
    """Print a step indicator."""
    print(f"\n{'-' * 70}")
    print(f"  Step {step_num}/{total}: {message}")
    print(f"{'-' * 70}")


def demo_complete_workflow(
    model_name: str | None = None,
    mode: str = "metadata-only",
    dry_run: bool = True,
):
    """
    Demonstrate the complete Fabric to Snowflake sync workflow.
    
    This function walks through each step of the sync process:
    1. Initialize pipeline
    2. Validate connections
    3. List available models
    4. Preview changes
    5. Execute sync
    """
    print(DEMO_BANNER)
    print(f"  Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Mode:      {mode}")
    print(f"  Dry Run:   {dry_run}")
    if model_name:
        print(f"  Model:     {model_name}")
    print()
    
    total_steps = 5
    
    # =========================================================================
    # Step 1: Initialize Pipeline
    # =========================================================================
    print_step(1, total_steps, "Initialize Pipeline")
    
    try:
        pipeline = FabricToSnowflakePipeline.from_env()
        print("  [OK] Pipeline initialized successfully")
        print("  [OK] Configuration loaded from environment")
    except Exception as e:
        print(f"  [FAIL] Failed to initialize pipeline: {e}")
        print("\n  Hint: Make sure .env file is configured with:")
        print("    - FABRIC_TENANT_ID")
        print("    - FABRIC_CLIENT_ID")
        print("    - FABRIC_CLIENT_SECRET")
        print("    - FABRIC_WORKSPACE_ID")
        print("    - SNOWFLAKE_ACCOUNT")
        print("    - SNOWFLAKE_USER")
        print("    - SNOWFLAKE_PASSWORD")
        return False
    
    # =========================================================================
    # Step 2: Validate Connections
    # =========================================================================
    print_step(2, total_steps, "Validate Connections")
    
    try:
        status = pipeline.validate_connections()
        
        fabric_status = "[OK] Connected" if status["fabric"] else "[FAIL] Failed"
        snowflake_status = "[OK] Connected" if status["snowflake"] else "[FAIL] Failed"
        
        print(f"  Fabric:    {fabric_status}")
        print(f"  Snowflake: {snowflake_status}")
        
        if not all(status.values()):
            print("\n  [WARN] Warning: Some connections failed")
            print("  The sync will proceed but may encounter errors.")
    except Exception as e:
        print(f"  [WARN] Connection validation error: {e}")
        print("  Proceeding with sync attempt...")
    
    # =========================================================================
    # Step 3: List Available Models
    # =========================================================================
    print_step(3, total_steps, "List Available Models")
    
    try:
        models = pipeline.list_available_models()
        print(f"  Found {len(models)} semantic models in workspace:")
        print()
        
        for i, model in enumerate(models[:5], 1):  # Show first 5
            push_flag = "[Push API]" if model["is_push_enabled"] else "[Standard]"
            print(f"  {i}. {model['name']}")
            print(f"     ID: {model['id'][:20]}...")
            print(f"     Type: {push_flag}")
            print()
        
        if len(models) > 5:
            print(f"  ... and {len(models) - 5} more models")
            
    except Exception as e:
        print(f"  [WARN] Could not list models: {e}")
        print("  This may be due to authentication or permission issues.")
    
    # =========================================================================
    # Step 4: Preview Changes
    # =========================================================================
    print_step(4, total_steps, "Preview Changes")
    
    try:
        print("  Analyzing semantic model structure...")
        changes = pipeline.preview_changes(model_name=model_name)
        summary = changes.summary()
        
        print(f"\n  Changes Summary:")
        print(f"  +-------------------+-------+")
        print(f"  | Change Type       | Count |")
        print(f"  +-------------------+-------+")
        print(f"  | Additions         | {summary['added']:>5} |")
        print(f"  | Modifications     | {summary['modified']:>5} |")
        print(f"  | Removals          | {summary['removed']:>5} |")
        print(f"  +-------------------+-------+")
        print(f"  | Total             | {summary['total']:>5} |")
        print(f"  +-------------------+-------+")
        
    except Exception as e:
        print(f"  [WARN] Could not preview changes: {e}")
    
    # =========================================================================
    # Step 5: Execute Sync
    # =========================================================================
    print_step(5, total_steps, "Execute Sync")
    
    if dry_run:
        print("  [DRY RUN] DRY RUN MODE - No changes will be applied")
    else:
        print("  [LIVE] LIVE MODE - Changes will be applied to Snowflake")
    
    print()
    
    try:
        result = pipeline.sync_semantic_model(
            model_name=model_name,
            mode=SyncMode(mode),
            dry_run=dry_run,
        )
        
        # The SyncResult __str__ already prints a nice summary
        # So we just need to report final status here
        
        print("\n" + "=" * 70)
        if result.success:
            print("  [OK] DEMO COMPLETED SUCCESSFULLY")
        else:
            print("  [FAIL] DEMO COMPLETED WITH ERRORS")
            if result.error_message:
                print(f"  Error: {result.error_message}")
        print("=" * 70)
        
        return result.success
        
    except Exception as e:
        print(f"  [FAIL] Sync failed: {e}")
        return False


def demo_quick_sync():
    """
    Demonstrate the quick sync convenience function.
    
    This shows the simplest way to perform a semantic sync.
    """
    print("\n" + "=" * 70)
    print("  QUICK SYNC DEMO")
    print("  ---------------")
    print("  Using the sync_fabric_to_snowflake() convenience function")
    print("=" * 70)
    
    result = sync_fabric_to_snowflake(dry_run=True)
    
    print(f"\n  Quick sync {'succeeded' if result.success else 'failed'}")
    print(f"  Sync ID: {result.sync_id}")
    print(f"  Duration: {result.duration_seconds:.2f}s")


def main():
    """Main entry point for demo."""
    parser = argparse.ArgumentParser(
        description="Demo: Fabric to Snowflake Semantic Data Sync",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python demo_fabric_to_snowflake.py --dry-run
  python demo_fabric_to_snowflake.py --model "SalesAnalytics"
  python demo_fabric_to_snowflake.py --mode full
  python demo_fabric_to_snowflake.py --quick

For more information, see the semantic_sync documentation.
        """,
    )
    
    parser.add_argument(
        "--model", "-m",
        type=str,
        default=None,
        help="Name of the semantic model to sync (uses default if not specified)",
    )
    
    parser.add_argument(
        "--mode",
        type=str,
        choices=["full", "incremental", "metadata-only"],
        default="metadata-only",
        help="Sync mode (default: metadata-only)",
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate sync without applying changes (recommended for first run)",
    )
    
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Run quick sync demo using convenience function",
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging",
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(level=log_level)
    
    # Run appropriate demo
    if args.quick:
        demo_quick_sync()
    else:
        success = demo_complete_workflow(
            model_name=args.model,
            mode=args.mode,
            dry_run=args.dry_run,
        )
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
