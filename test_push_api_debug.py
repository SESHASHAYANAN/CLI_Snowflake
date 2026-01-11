"""Debug script to test Push API table creation."""

from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.config.settings import get_settings

settings = get_settings()
fabric_config = settings.get_fabric_config()
client = FabricClient(fabric_config)

dataset_id = fabric_config.dataset_id
workspace_id = fabric_config.workspace_id

# Check dataset details
print("=== Dataset Details ===")
resp = client.get_dataset(dataset_id)
print(f"addRowsAPIEnabled: {resp.get('addRowsAPIEnabled')}")
print(f"isRefreshable: {resp.get('isRefreshable')}")
print(f"targetStorageMode: {resp.get('targetStorageMode')}")

# Check existing tables
print("\n=== Existing Tables ===")
tables = client.get_dataset_tables(dataset_id)
for t in tables:
    print(f"  - {t.get('name')}")

# Try PUT to update existing table (should work for Push API)
print("\n=== Test PUT on existing table ===")
try:
    test_table = {
        "name": "SnowflakeData",
        "columns": [
            {"name": "ID", "dataType": "Int64"},
            {"name": "Name", "dataType": "String"},
            {"name": "TestColumn", "dataType": "String"}
        ]
    }
    result = client.put(f"/groups/{workspace_id}/datasets/{dataset_id}/tables/SnowflakeData", data=test_table)
    print(f"PUT Success: {result}")
except Exception as e:
    print(f"PUT Error: {e}")

# Try POST to create new table (this has been failing)
print("\n=== Test POST for new table ===")
try:
    new_table = {
        "name": "TestNewTable",
        "columns": [
            {"name": "ID", "dataType": "Int64"},
            {"name": "Name", "dataType": "String"}
        ]
    }
    result = client.post(f"/groups/{workspace_id}/datasets/{dataset_id}/tables", data=new_table)
    print(f"POST Success: {result}")
except Exception as e:
    print(f"POST Error: {e}")
