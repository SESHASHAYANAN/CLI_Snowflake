"""List all Push API datasets created"""
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.config.settings import load_settings

s = load_settings()
c = FabricClient(s.get_fabric_config())
ds = c.list_workspace_datasets()

push = [d for d in ds if d.get('addRowsAPIEnabled') or '_PushSync' in d.get('name', '')]
print('Push API Datasets Created:')
for d in push:
    print(f"  [OK] {d['name']} (ID: {d['id']})")
