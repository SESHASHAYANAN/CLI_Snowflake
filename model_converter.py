"""
REST API to Push API Converter

Converts any Fabric semantic model (Import/DirectQuery/LiveConnection) 
to Push API format JSON, then creates a Push API dataset that can sync to Snowflake.

Main flow:
1. Detect new model in Fabric
2. Read its schema (via REST API, XMLA, or BIM)
3. Convert to Push API JSON format
4. Create new Push API dataset with suffix "_PushSync"
5. Automatically sync to Snowflake using CLI
"""

import sys
import os
import json
import subprocess
from datetime import datetime

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from semantic_sync.config.settings import load_settings
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.core.fabric_model_parser import FabricModelParser
from semantic_sync.auth.oauth import FabricOAuthClient
import requests


class ModelConverter:
    """Converts Fabric models to Push API format."""
    
    def __init__(self):
        self.settings = load_settings()
        self.fabric_config = self.settings.get_fabric_config()
        self.fabric_client = FabricClient(self.fabric_config)
        self.parser = FabricModelParser(self.fabric_client, self.fabric_config)
        
        # OAuth client for API calls
        self.oauth_client = FabricOAuthClient(config=self.fabric_config)
        self.workspace_id = self.fabric_config.workspace_id
    
    def map_data_type_to_push_api(self, data_type: str) -> str:
        """
        Map semantic model data types to Push API data types.
        
        Args:
            data_type: Original data type (from model)
            
        Returns:
            Push API compatible data type
        """
        data_type_upper = data_type.upper()
        
        # Push API supported types: Int64, Double, Boolean, Datetime, String
        if data_type_upper in ['INT', 'INTEGER', 'INT64', 'LONG']:
            return 'Int64'
        elif data_type_upper in ['FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC', 'REAL', 'CURRENCY']:
            return 'Double'
        elif data_type_upper in ['BOOL', 'BOOLEAN']:
            return 'Boolean'
        elif data_type_upper in ['DATE', 'DATETIME', 'TIMESTAMP', 'TIME']:
            return 'Datetime'
        else:
            return 'String'  # Default to String for text and unknown types
    
    def convert_model_to_push_api_json(self, dataset_id: str) -> dict:
        """
        Read a Fabric model and convert to Push API JSON format.
        
        Args:
            dataset_id: Fabric dataset ID
            
        Returns:
            Push API dataset definition (JSON)
        """
        print(f"📖 Reading model schema for dataset {dataset_id}...")
        
        # Parse the semantic model
        model = self.parser.read_semantic_model(dataset_id=dataset_id)
        
        print(f"   Model: {model.name}")
        print(f"   Tables: {len(model.tables)}")
        
        if not model.tables:
            raise ValueError(f"Model '{model.name}' has no tables to convert")
        
        # Convert to Push API format
        push_api_tables = []
        
        for table in model.tables:
            push_table = {
                "name": table.name,
                "columns": []
            }
            
            # Convert columns
            for column in table.columns:
                push_column = {
                    "name": column.name,
                    "dataType": self.map_data_type_to_push_api(column.data_type)
                }
                push_table["columns"].append(push_column)
            
            push_api_tables.append(push_table)
            print(f"   ✓ Converted table: {table.name} ({len(table.columns)} columns)")
        
        # Create Push API dataset definition
        push_dataset = {
            "name": f"{model.name}_PushSync",
            "defaultMode": "Push",
            "tables": push_api_tables
        }
        
        return push_dataset
    
    def create_push_api_dataset(self, push_dataset_def: dict) -> str:
        """
        Create a Push API dataset in Fabric.
        
        Args:
            push_dataset_def: Push API dataset definition
            
        Returns:
            Dataset ID of created dataset
        """
        print(f"\n📤 Creating Push API dataset: {push_dataset_def['name']}")
        
        token = self.oauth_client.get_access_token()
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets"
        
        response = requests.post(url, headers=headers, json=push_dataset_def)
        
        if response.status_code in [200, 201]:
            dataset = response.json()
            dataset_id = dataset.get("id")
            print(f"   ✅ Created successfully!")
            print(f"   Dataset ID: {dataset_id}")
            return dataset_id
        else:
            error_msg = f"Failed to create Push API dataset: {response.status_code}\n{response.text}"
            print(f"   ❌ {error_msg}")
            raise Exception(error_msg)
    
    def sync_to_snowflake(self):
        """Run semantic-sync CLI to sync all models to Snowflake."""
        print("\n🚀 Syncing to Snowflake using CLI...")
        print("-" * 70)
        
        result = subprocess.run(
            ["semantic-sync", "sync", "--direction", "fabric-to-snowflake"],
            capture_output=True,
            text=True,
            encoding='utf-8'
        )
        
        if result.stdout:
            print(result.stdout)
        
        if result.returncode == 0:
            print("✅ Sync completed successfully")
            return True
        else:
            print(f"❌ Sync failed with return code {result.returncode}")
            if result.stderr:
                print(f"Error: {result.stderr}")
            return False
    
    def convert_and_sync(self, dataset_id: str, dataset_name: str):
        """
        Complete flow: Convert model to Push API and sync to Snowflake.
        
        Args:
            dataset_id: Source dataset ID
            dataset_name: Source dataset name
        """
        print("\n" + "="*70)
        print(f"🔄 CONVERTING MODEL TO PUSH API: {dataset_name}")
        print("="*70)
        
        try:
            # Step 1: Convert to Push API JSON
            push_dataset_def = self.convert_model_to_push_api_json(dataset_id)
            
            # Step 2: Save JSON for reference
            json_file = f"{dataset_name}_push_api.json"
            with open(json_file, 'w') as f:
                json.dump(push_dataset_def, f, indent=2)
            print(f"\n💾 Saved Push API JSON to: {json_file}")
            
            # Step 3: Create Push API dataset
            new_dataset_id = self.create_push_api_dataset(push_dataset_def)
            
            # Step 4: Sync to Snowflake
            self.sync_to_snowflake()
            
            print("\n" + "="*70)
            print("✅ CONVERSION AND SYNC COMPLETE!")
            print("="*70)
            print(f"Original Model: {dataset_name} ({dataset_id})")
            print(f"Push API Model: {push_dataset_def['name']} ({new_dataset_id})")
            print(f"Snowflake: Synced to {self.settings.get_snowflake_config().database}")
            
        except Exception as e:
            print("\n" + "="*70)
            print(f"❌ FAILED: {e}")
            print("="*70)
            raise
    
    def auto_convert_new_models(self):
        """
        Automatic mode: Find models without Push API versions and convert them.
        """
        print("\n" + "="*70)
        print("🤖 AUTO-CONVERT MODE: Finding models to convert")
        print("="*70)
        
        # Get all datasets
        all_datasets = self.fabric_client.list_workspace_datasets()
        
        # Separate Push API and non-Push API datasets
        push_datasets = {}
        non_push_datasets = {}
        
        for ds in all_datasets:
            ds_id = ds['id']
            ds_name = ds.get('name', 'Unknown')
            is_push = ds.get('addRowsAPIEnabled', False)
            
            if is_push:
                # Remove "_PushSync" suffix for comparison
                base_name = ds_name.replace('_PushSync', '')
                push_datasets[base_name] = ds
            else:
                non_push_datasets[ds_name] = ds
        
        # Find models that don't have Push API versions
        models_to_convert = []
        for name, ds in non_push_datasets.items():
            if name not in push_datasets:
                models_to_convert.append(ds)
        
        if not models_to_convert:
            print("\n✅ All models already have Push API versions!")
            return
        
        print(f"\n📋 Found {len(models_to_convert)} models to convert:")
        for ds in models_to_convert:
            print(f"   - {ds.get('name')}")
        
        # Convert each model
        for ds in models_to_convert:
            try:
                self.convert_and_sync(ds['id'], ds.get('name'))
            except Exception as e:
                print(f"\n⚠️  Skipping {ds.get('name')}: {e}")
                continue


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Convert Fabric models to Push API format and sync to Snowflake"
    )
    parser.add_argument(
        '--dataset-id',
        help='Specific dataset ID to convert'
    )
    parser.add_argument(
        '--dataset-name',
        help='Specific dataset name (used with --dataset-id)'
    )
    parser.add_argument(
        '--auto',
        action='store_true',
        help='Auto-convert all models without Push API versions'
    )
    
    args = parser.parse_args()
    
    converter = ModelConverter()
    
    if args.auto:
        converter.auto_convert_new_models()
    elif args.dataset_id:
        name = args.dataset_name or args.dataset_id
        converter.convert_and_sync(args.dataset_id, name)
    else:
        print("Usage:")
        print("  python model_converter.py --auto")
        print("  python model_converter.py --dataset-id <ID> --dataset-name <NAME>")
        sys.exit(1)


if __name__ == "__main__":
    main()
