"""
Explore ALL Fabric REST APIs to find readable items.

Fabric has multiple APIs:
- Power BI API (datasets, reports)
- Fabric Items API (lakehouses, warehouses, notebooks)
- OneLake API (file storage)
"""

import sys
import os

sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests


def main():
    print("=" * 70)
    print("EXPLORING ALL FABRIC APIs")
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

    # ============================================================
    # 1. Power BI Datasets API
    # ============================================================
    print("=" * 50)
    print("1. POWER BI DATASETS")
    print("=" * 50)
    
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        datasets = response.json().get("value", [])
        print(f"Found {len(datasets)} datasets")
        for ds in datasets:
            print(f"  - {ds['name']} (ID: {ds['id']})")
    else:
        print(f"Error: {response.status_code}")
    print()

    # ============================================================
    # 2. Fabric Items API (v1)
    # ============================================================
    print("=" * 50)
    print("2. FABRIC ITEMS API")
    print("=" * 50)
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/items"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        items = response.json().get("value", [])
        print(f"Found {len(items)} items")
        
        # Group by type
        by_type = {}
        for item in items:
            item_type = item.get("type", "Unknown")
            if item_type not in by_type:
                by_type[item_type] = []
            by_type[item_type].append(item)
        
        for item_type, type_items in by_type.items():
            print(f"\n  [{item_type}] - {len(type_items)} items")
            for item in type_items:
                print(f"    - {item.get('displayName')} (ID: {item.get('id')})")
    else:
        print(f"Error: {response.status_code} - {response.text[:200]}")
    print()

    # ============================================================
    # 3. Semantic Models API (Fabric-specific)
    # ============================================================
    print("=" * 50)
    print("3. SEMANTIC MODELS API")
    print("=" * 50)
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        models = response.json().get("value", [])
        print(f"Found {len(models)} semantic models")
        for model in models:
            print(f"  - {model.get('displayName')} (ID: {model.get('id')})")
            
            # Try to get model definition
            model_id = model.get("id")
            def_url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/semanticModels/{model_id}/getDefinition"
            def_response = requests.post(def_url, headers=headers)
            if def_response.status_code == 200:
                print(f"    [OK] Can read definition!")
            else:
                print(f"    Definition: {def_response.status_code}")
    else:
        print(f"Error: {response.status_code} - {response.text[:200]}")
    print()

    # ============================================================
    # 4. Lakehouses API
    # ============================================================
    print("=" * 50)
    print("4. LAKEHOUSES API")
    print("=" * 50)
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/lakehouses"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        lakehouses = response.json().get("value", [])
        print(f"Found {len(lakehouses)} lakehouses")
        for lh in lakehouses:
            print(f"  - {lh.get('displayName')} (ID: {lh.get('id')})")
    else:
        print(f"Error: {response.status_code}")
    print()

    # ============================================================
    # 5. Warehouses API
    # ============================================================
    print("=" * 50)
    print("5. WAREHOUSES API")
    print("=" * 50)
    
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/warehouses"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        warehouses = response.json().get("value", [])
        print(f"Found {len(warehouses)} warehouses")
        for wh in warehouses:
            print(f"  - {wh.get('displayName')} (ID: {wh.get('id')})")
    else:
        print(f"Error: {response.status_code}")
    print()

    # ============================================================
    # 6. Try to get table data via executeQueries
    # ============================================================
    print("=" * 50)
    print("6. TESTING DATA ACCESS")
    print("=" * 50)
    
    # Re-get datasets
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    response = requests.get(url, headers=headers)
    datasets = response.json().get("value", [])
    
    for ds in datasets[:3]:  # Test first 3
        ds_id = ds["id"]
        ds_name = ds["name"]
        print(f"\nTesting: {ds_name}")
        
        # Try different queries
        queries = [
            ("Tables via EVALUATE", "EVALUATE TOPN(1, VALUES(INFO.TABLES()))"),
            ("Sample data", "EVALUATE SAMPLE(1, ALL(VALUES(INFO.TABLES())))"),
        ]
        
        for query_name, query in queries:
            url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{ds_id}/executeQueries"
            payload = {
                "queries": [{"query": query}],
                "serializerSettings": {"includeNulls": True}
            }
            
            headers_json = {**headers, "Content-Type": "application/json"}
            response = requests.post(url, headers=headers_json, json=payload)
            
            if response.status_code == 200:
                result = response.json()
                rows = result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                print(f"  {query_name}: [OK] {len(rows)} rows")
            else:
                print(f"  {query_name}: Status {response.status_code}")

    print()
    print("=" * 70)
    print("EXPLORATION COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
