"""
Auto Model Converter and Sync
Detects all Fabric models, converts to Push API, and syncs to Snowflake
"""
import sys
import os
import json
import subprocess

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

from semantic_sync.config.settings import load_settings
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.core.fabric_model_parser import FabricModelParser
from semantic_sync.auth.oauth import FabricOAuthClient
import requests


def map_data_type(data_type: str) -> str:
    """Map data types to Push API format."""
    dt = data_type.upper()
    if dt in ['INT', 'INTEGER', 'INT64', 'LONG']:
        return 'Int64'
    elif dt in ['FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC']:
        return 'Double'
    elif dt in ['BOOL', 'BOOLEAN']:
        return 'Boolean'
    elif dt in ['DATE', 'DATETIME', 'TIMESTAMP']:
        return 'Datetime'
    return 'String'


def main():
    print("\n" + "="*70)
    print("🤖 AUTO MODEL CONVERTER")
    print("="*70)
    
    # Debug Registry
    try:
        from semantic_sync.core.metadata_registry import get_metadata_registry
        reg = get_metadata_registry()
        print(f"   🔍 Registry loaded from: {reg.registry_dir}")
        print(f"   🔍 Demo Table in registry? {reg.has_manual_definition('demo Table')}")
    except ImportError as e:
        print(f"   ❌ Failed to import registry: {e}")
        
    # Initialize
    settings = load_settings()
    fabric_config = settings.get_fabric_config()
    fabric_client = FabricClient(fabric_config)
    parser = FabricModelParser(fabric_client, fabric_config)
    oauth = FabricOAuthClient(config=fabric_config)
    workspace_id = fabric_config.workspace_id
    
    # Get all datasets
    print("\n📊 Fetching all datasets from Fabric...")
    datasets = fabric_client.list_workspace_datasets()
    
    # Separate Push and non-Push
    push_names = set()
    to_convert = []
    
    for ds in datasets:
        name = ds.get('name', '')
        is_push = ds.get('addRowsAPIEnabled', False)
        
        if is_push:
            push_names.add(name.replace('_PushSync', ''))
        else:
            if not name.endswith('_PushSync'):
                to_convert.append(ds)
    
    # Filter out already converted
    to_convert = [ds for ds in to_convert if ds.get('name') not in push_names]
    
    print(f"   Total: {len(datasets)} datasets")
    print(f"   Push API: {len(push_names)}")
    print(f"   To convert: {len(to_convert)}")
    
    if not to_convert:
        print("\n✅ All models already have Push API versions!")
        return
    
    print(f"\n📋 Models to convert:")
    for ds in to_convert:
        print(f"   - {ds.get('name')}")
    
    # Convert each
    token = oauth.get_access_token()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    success = 0
    failed = 0
    
    for ds in to_convert:
        ds_id = ds['id']
        ds_name = ds.get('name', 'Unknown')
        
        print(f"\n{'='*70}")
        print(f"🔄 Converting: {ds_name}")
        print("="*70)
        
        try:
            # Read model
            print("   Reading schema...")
            
            # FAST PATH: Check manual registry first to avoid slow API calls
            model = None
            try:
                from semantic_sync.core.metadata_registry import get_metadata_registry
                from semantic_sync.core.models import SemanticModel
                reg = get_metadata_registry()
                
                print(f"   🔍 Checking registry for '{ds_name}'...")
                if reg.has_manual_definition(ds_name):
                    print(f"   ⚡ Fast Path: Found manual definition for '{ds_name}'")
                    tables = reg.get_manual_tables(ds_name)
                    desc = reg.get_manual_description(ds_name)
                    model = SemanticModel(
                        name=ds_name,
                        id=ds_id,
                        source="fabric",
                        description=desc,
                        tables=tables,
                        measures=[],
                        relationships=[]
                    )
            except Exception as e:
                print(f"   ⚠️  Fast path failed: {e}")
            
            if not model:
               model = parser.read_semantic_model(dataset_id=ds_id)
            
            
            if not model.tables:
                print(f"   ⚠️  No tables found, skipping")
                continue
            
            print(f"   Found {len(model.tables)} tables")
            
            # Convert to Push API format
            push_tables = []
            for table in model.tables:
                push_table = {
                    "name": table.name,
                    "columns": [
                        {"name": col.name, "dataType": map_data_type(col.data_type)}
                        for col in table.columns
                    ]
                }
                push_tables.append(push_table)
                print(f"   ✓ {table.name} ({len(table.columns)} columns)")
            
            # Create Push API dataset
            push_def = {
                "name": f"{ds_name}_PushSync",
                "defaultMode": "Push",
                "tables": push_tables
            }
            
            # Save JSON
            with open(f"{ds_name}_push_api.json", 'w') as f:
                json.dump(push_def, f, indent=2)
            print(f"   💾 Saved: {ds_name}_push_api.json")
            
            # Create in Fabric
            print(f"   📤 Creating Push API dataset...")
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
            resp = requests.post(url, headers=headers, json=push_def)
            
            if resp.status_code in [200, 201]:
                new_id = resp.json().get('id')
                print(f"   ✅ Created! ID: {new_id}")
                success += 1
            else:
                print(f"   ❌ Failed: {resp.status_code} - {resp.text[:100]}")
                failed += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            failed += 1
    
    # Run sync
    print("\n" + "="*70)
    print("🚀 Running Snowflake sync...")
    print("="*70)
    
    result = subprocess.run(
        ["semantic-sync", "sync", "--direction", "fabric-to-snowflake", "--force"],
        capture_output=True, text=True, encoding='utf-8'
    )
    
    if result.stdout:
        print(result.stdout)
    
    # Summary
    print("\n" + "="*70)
    print("📊 SUMMARY")
    print("="*70)
    print(f"   Converted: {success}")
    print(f"   Failed: {failed}")
    print(f"   Synced to Snowflake: {'✅' if result.returncode == 0 else '❌'}")
    print("="*70)


if __name__ == "__main__":
    main()
