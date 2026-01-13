"""Delete demo Table_PushSync"""
import requests
from semantic_sync.core.fabric_client import FabricClient
from semantic_sync.auth.oauth import FabricOAuthClient
from semantic_sync.config.settings import load_settings

s = load_settings()
config = s.get_fabric_config()
c = FabricClient(config)
oauth = FabricOAuthClient(config)

token = oauth.get_access_token()
headers = {"Authorization": f"Bearer {token}"}

ds = c.list_workspace_datasets()
for d in ds:
    if d['name'] == 'demo Table_PushSync':
        print(f"Deleting {d['name']} ({d['id']})...")
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{config.workspace_id}/datasets/{d['id']}"
        requests.delete(url, headers=headers)
        print("Deleted.")
