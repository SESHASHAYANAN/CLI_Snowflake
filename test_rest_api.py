"""Test REST API access to Fabric datasets."""

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests

settings = get_settings()
fabric_config = settings.get_fabric_config()
workspace_id = fabric_config.workspace_id

# Authenticate
print("Authenticating...")
oauth = FabricOAuthClient(config=fabric_config)
token = oauth.get_access_token()
headers = {"Authorization": f"Bearer {token}"}
print("[OK]")

# Get datasets
url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
resp = requests.get(url, headers=headers)
print(f"\nDatasets API: {resp.status_code}")
datasets = resp.json().get("value", [])

for ds in datasets:
    ds_id = ds["id"]
    ds_name = ds["name"]
    print(f"\n=== {ds_name} ===")
    
    # Try tables endpoint
    tables_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{ds_id}/tables"
    tables_resp = requests.get(tables_url, headers=headers)
    print(f"  Tables API: {tables_resp.status_code}")
    
    if tables_resp.status_code == 200:
        tables = tables_resp.json().get("value", [])
        print(f"  Tables found: {len(tables)}")
        for t in tables:
            cols = t.get("columns", [])
            print(f"    - {t.get('name')}: {len(cols)} columns")
            for c in cols[:3]:
                print(f"        * {c.get('name')} ({c.get('dataType')})")
    else:
        print(f"  Response: {tables_resp.text[:200]}")
