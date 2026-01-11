"""
Debug: Print full API responses to understand structure.
"""

import sys
import os
import time
import json

sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests


def main():
    settings = get_settings()
    fabric_config = settings.get_fabric_config()
    workspace_id = fabric_config.workspace_id

    # Authenticate
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    # Get first model
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels"
    response = requests.get(url, headers=headers)
    models = response.json().get("value", [])
    
    if not models:
        print("No models found")
        return
    
    model = models[0]  # First model
    model_id = model.get("id")
    model_name = model.get("displayName")
    
    print(f"Testing model: {model_name}")
    print(f"ID: {model_id}")
    print()

    # Try getDefinition with format parameter
    print("1. Trying getDefinition with TMDL format...")
    def_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition?format=TMDL"
    response = requests.post(def_url, headers=headers)
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 202:
        op_url = response.headers.get("Location")
        if op_url:
            time.sleep(3)
            poll = requests.get(op_url, headers=headers)
            print(f"   Poll Status: {poll.status_code}")
            if poll.status_code == 200:
                result = poll.json()
                print(f"   Result keys: {result.keys()}")
                print(json.dumps(result, indent=2)[:2000])
    print()

    # Try without format
    print("2. Trying getDefinition without format...")
    def_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition"
    response = requests.post(def_url, headers=headers)
    print(f"   Status: {response.status_code}")
    
    if response.status_code == 202:
        op_url = response.headers.get("Location")
        if op_url:
            time.sleep(3)
            poll = requests.get(op_url, headers=headers)
            print(f"   Poll Status: {poll.status_code}")
            if poll.status_code == 200:
                result = poll.json()
                print(f"   Result keys: {result.keys()}")
                definition = result.get("definition", {})
                print(f"   Definition keys: {definition.keys() if definition else 'None'}")
    print()

    # Try getting tables via Power BI Admin API
    print("3. Trying Power BI Admin API for table info...")
    admin_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{model_id}"
    response = requests.get(admin_url, headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))
    print()

    # Try tables endpoint
    print("4. Trying tables endpoint...")
    tables_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{model_id}/tables"
    response = requests.get(tables_url, headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))
    print()

    # Try refresh info (sometimes contains schema)
    print("5. Trying refreshes endpoint...")
    refresh_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{model_id}/refreshes"
    response = requests.get(refresh_url, headers=headers)
    print(f"   Status: {response.status_code}")
    print()

    # Try datasources
    print("6. Trying datasources endpoint...")
    ds_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{model_id}/datasources"
    response = requests.get(ds_url, headers=headers)
    print(f"   Status: {response.status_code}")
    if response.status_code == 200:
        print(json.dumps(response.json(), indent=2))


if __name__ == "__main__":
    main()
