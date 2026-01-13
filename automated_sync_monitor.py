"""
Automated Bi-Directional Sync Monitor

This script continuously monitors for:
1. New semantic models created in Fabric → Auto-sync to Snowflake using CLI
2. Changes to existing models in Fabric → Auto-sync updates to Snowflake

Uses the semantic-sync CLI commands for all operations.
Run this script in the background to enable automatic synchronization.
"""

import sys
import os
import time
import json
import subprocess
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from semantic_sync.config import get_settings
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.utils.logger import setup_logging
from model_converter import ModelConverter

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

# State file to track synced models
STATE_FILE = Path("sync_state.json")


class SyncMonitor:
    """Monitors and automatically syncs Fabric models to Snowflake using CLI."""
    
    def __init__(self, check_interval_seconds=300):
        """
        Initialize the sync monitor.
        
        Args:
            check_interval_seconds: How often to check for new models (default: 5 minutes)
        """
        self.check_interval = check_interval_seconds
        self.settings = get_settings()
        self.fabric_config = self.settings.get_fabric_config()
        self.snowflake_config = self.settings.get_snowflake_config()
        self.fabric_client = FabricClient(self.fabric_config)
        
        # Initialize model converter
        self.converter = ModelConverter()
        
        # Load or initialize state
        self.state = self.load_state()
        
    def load_state(self):
        """Load the sync state from file."""
        if STATE_FILE.exists():
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")
                return {"synced_models": {}, "last_check": None}
        return {"synced_models": {}, "last_check": None}
    
    def save_state(self):
        """Save the current sync state to file."""
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")
    
    def get_fabric_models(self):
        """Get all semantic models from Fabric workspace."""
        try:
            datasets = self.fabric_client.list_workspace_datasets()
            return {ds['id']: ds for ds in datasets}
        except Exception as e:
            logger.error(f"Failed to list Fabric datasets: {e}")
            return {}
    
    def run_cli_sync(self):
        """
        Run the semantic-sync CLI command to sync all models.
        
        Returns:
            bool: True if sync successful, False otherwise
        """
        try:
            logger.info("Running: semantic-sync sync --direction fabric-to-snowflake")
            
            # Run the CLI command
            result = subprocess.run(
                ["semantic-sync", "sync", "--direction", "fabric-to-snowflake"],
                capture_output=True,
                text=True,
                encoding='utf-8'
            )
            
            # Print output
            if result.stdout:
                print(result.stdout)
            
            if result.stderr:
                logger.error(f"CLI stderr: {result.stderr}")
            
            if result.returncode == 0:
                logger.info("✅ CLI sync completed successfully")
                return True
            else:
                logger.error(f"❌ CLI sync failed with return code {result.returncode}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to run CLI sync: {e}")
            return False
    
    def check_and_sync(self):
        """Check for new/updated models and sync them using CLI."""
        print("\n" + "="*70)
        print(f"🔄 Sync Check - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("="*70)
        
        # Get current models from Fabric
        current_models = self.get_fabric_models()
        
        if not current_models:
            print("⚠️  No models found in Fabric workspace")
            return
        
        print(f"📊 Found {len(current_models)} models in Fabric workspace")
        
        # Check for new or updated models
        synced_models = self.state.get("synced_models", {})
        new_models = []
        updated_models = []
        
        for model_id, model_data in current_models.items():
            model_name = model_data.get('name', 'Unknown')
            created_date = model_data.get('createdDate')
            modified_date = model_data.get('modifiedDateTime', created_date)
            
            if model_id not in synced_models:
                # New model
                new_models.append((model_id, model_name, created_date))
            else:
                # Check if modified
                last_synced = synced_models[model_id].get('last_sync_time')
                if modified_date and last_synced and modified_date > last_synced:
                    updated_models.append((model_id, model_name, modified_date))
        
        # Report findings
        if new_models:
            print(f"\n🆕 New models detected: {len(new_models)}")
            for model_id, model_name, created_date in new_models:
                print(f"   - {model_name} (created: {created_date})")
        
        if updated_models:
            print(f"\n🔄 Updated models detected: {len(updated_models)}")
            for model_id, model_name, modified_date in updated_models:
                print(f"   - {model_name} (modified: {modified_date})")
        
        if not new_models and not updated_models:
            print("\n✅ No new or updated models - all in sync")
            self.state['last_check'] = datetime.now().isoformat()
            self.save_state()
            return
        
        # Auto-convert new non-Push API models
        if new_models:
            print(f"\n🔄 Checking if models need Push API conversion...")
            for model_id, model_name, created_date in new_models:
                model_data = current_models[model_id]
                is_push = model_data.get('addRowsAPIEnabled', False)
                
                if not is_push and not model_name.endswith('_PushSync'):
                    print(f"\n📋 Converting non-Push API model: {model_name}")
                    try:
                        self.converter.convert_and_sync(model_id, model_name)
                    except Exception as e:
                        logger.warning(f"Failed to convert {model_name}: {e}")
                        print(f"   ⚠️  Skipping conversion: {e}")
                        # Continue with normal sync anyway
        
        # Run sync using CLI
        print(f"\n🚀 Running sync using semantic-sync CLI...")
        print("-" * 70)
        
        if self.run_cli_sync():
            # Update state for all current models
            for model_id, model_data in current_models.items():
                model_name = model_data.get('name', 'Unknown')
                synced_models[model_id] = {
                    'name': model_name,
                    'last_sync_time': datetime.now().isoformat(),
                    'sync_count': synced_models.get(model_id, {}).get('sync_count', 0) + 1
                }
            
            # Save updated state
            self.state['synced_models'] = synced_models
            self.state['last_check'] = datetime.now().isoformat()
            self.save_state()
            
            print("\n" + "="*70)
            print(f"✅ Sync completed successfully")
            print("="*70)
        else:
            print("\n" + "="*70)
            print(f"❌ Sync failed - check logs for details")
            print("="*70)
    
    def run_continuous(self):
        """Run the monitor continuously."""
        print("\n" + "="*70)
        print("🤖 AUTOMATED SYNC MONITOR STARTED")
        print("="*70)
        print(f"Workspace: {self.fabric_config.workspace_id}")
        print(f"Snowflake: {self.snowflake_config.database}.{self.snowflake_config.schema_name}")
        print(f"Check interval: {self.check_interval} seconds ({self.check_interval/60:.1f} minutes)")
        print(f"Using CLI: semantic-sync sync --direction fabric-to-snowflake")
        print("\nPress Ctrl+C to stop...")
        print("="*70)
        
        try:
            while True:
                try:
                    self.check_and_sync()
                except Exception as e:
                    logger.error(f"Error during sync check: {e}")
                    print(f"\n❌ Error: {e}")
                
                # Wait for next check
                next_check = datetime.now() + timedelta(seconds=self.check_interval)
                print(f"\n⏰ Next check: {next_check.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"💤 Sleeping for {self.check_interval} seconds...")
                
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            print("\n\n🛑 Monitor stopped by user")
            print("="*70)
            sys.exit(0)
    
    def run_once(self):
        """Run a single sync check (useful for scheduled tasks)."""
        try:
            self.check_and_sync()
        except Exception as e:
            logger.error(f"Error during sync check: {e}")
            print(f"\n❌ Error: {e}")
            sys.exit(1)


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Automated Fabric to Snowflake Sync Monitor (uses semantic-sync CLI)"
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=300,
        help='Check interval in seconds (default: 300 = 5 minutes)'
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit (for scheduled tasks)'
    )
    
    args = parser.parse_args()
    
    monitor = SyncMonitor(check_interval_seconds=args.interval)
    
    if args.once:
        monitor.run_once()
    else:
        monitor.run_continuous()


if __name__ == "__main__":
    main()
