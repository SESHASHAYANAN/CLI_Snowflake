"""
Get Dataset ID for 'Employee' model
"""
import sys
import os
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.config.settings import load_settings

# Fix encoding
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

try:
    settings = load_settings()
    client = FabricClient(settings.config.get_fabric_config() if hasattr(settings, 'config') else settings.get_fabric_config())
    
    print(f"Listing datasets in workspace: {settings.fabric_workspace_id}...")
    datasets = client.list_workspace_datasets()
    
    employee_ds = next((d for d in datasets if d.get('name') == 'Employee'), None)
    
    if employee_ds:
        print(f"\n[FOUND] Employee Dataset:")
        print(f"  ID:   {employee_ds['id']}")
        print(f"  Name: {employee_ds['name']}")
        print(f"  Type: {employee_ds.get('targetStorageMode', 'Unknown')}")
    else:
        print("\n[NOT FOUND] 'Employee' dataset not found in this workspace.")
        
except Exception as e:
    print(f"Error: {e}")
