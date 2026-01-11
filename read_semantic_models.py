"""
Read Semantic Model definitions using Fabric REST API.

The getDefinition endpoint returns 202 (async operation).
We need to poll for the result.
"""

import sys
import os
import time
import json
import base64

sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests


def main():
    print("=" * 70)
    print("READING SEMANTIC MODEL DEFINITIONS")
    print("=" * 70)
    print()

    settings = get_settings()
    fabric_config = settings.get_fabric_config()
    workspace_id = fabric_config.workspace_id

    # Authenticate
    print("Authenticating...")
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    headers = {"Authorization": f"Bearer {token}"}
    print("[OK]")
    print()

    # Get semantic models
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels"
    response = requests.get(url, headers=headers)
    models = response.json().get("value", [])
    
    print(f"Found {len(models)} semantic models")
    print()

    for model in models:
        model_id = model.get("id")
        model_name = model.get("displayName")
        
        print(f"Reading: {model_name}")
        
        # Request definition (async)
        def_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition"
        response = requests.post(def_url, headers=headers)
        
        if response.status_code == 202:
            # Async - get operation location
            operation_url = response.headers.get("Location")
            retry_after = int(response.headers.get("Retry-After", 2))
            
            if operation_url:
                print(f"  Polling for result...")
                
                # Poll for result
                for attempt in range(10):
                    time.sleep(retry_after)
                    
                    poll_response = requests.get(operation_url, headers=headers)
                    
                    if poll_response.status_code == 200:
                        result = poll_response.json()
                        status = result.get("status")
                        
                        if status == "Succeeded":
                            definition = result.get("definition", {})
                            parts = definition.get("parts", [])
                            
                            print(f"  [OK] Got definition with {len(parts)} parts")
                            
                            # Look for model.bim or database.json
                            for part in parts:
                                path = part.get("path", "")
                                payload = part.get("payload", "")
                                
                                if "model" in path.lower() or "database" in path.lower():
                                    print(f"    Found: {path}")
                                    
                                    # Decode base64
                                    try:
                                        decoded = base64.b64decode(payload).decode('utf-8')
                                        data = json.loads(decoded)
                                        
                                        # Extract tables
                                        if "model" in data:
                                            tables = data["model"].get("tables", [])
                                        else:
                                            tables = data.get("tables", [])
                                        
                                        print(f"    Tables found: {len(tables)}")
                                        
                                        for table in tables:
                                            table_name = table.get("name", "Unknown")
                                            columns = table.get("columns", [])
                                            
                                            # Skip hidden/system tables
                                            if table_name.startswith("DateTableTemplate") or table_name.startswith("LocalDateTable"):
                                                continue
                                            
                                            print(f"\n    TABLE: {table_name}")
                                            for col in columns:
                                                col_name = col.get("name", "?")
                                                col_type = col.get("dataType", "?")
                                                print(f"      - {col_name} ({col_type})")
                                    except Exception as e:
                                        print(f"    Error parsing: {e}")
                            
                            break
                        elif status == "Failed":
                            error = result.get("error", {})
                            print(f"  [FAILED] {error.get('message', 'Unknown error')}")
                            break
                        else:
                            print(f"  Status: {status}")
                    elif poll_response.status_code == 202:
                        # Still processing
                        continue
                    else:
                        print(f"  Poll error: {poll_response.status_code}")
                        break
            else:
                print(f"  No operation URL returned")
        else:
            print(f"  Status: {response.status_code}")
        
        print()

    print("=" * 70)
    print("COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
