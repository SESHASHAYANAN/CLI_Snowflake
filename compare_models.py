"""
Compare different Fabric models to understand why some work and others don't
"""
import os
import sys
import requests
from dotenv import load_dotenv

# Fix encoding for Windows console
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# Load environment variables
load_dotenv()

# Fabric authentication
tenant_id = os.getenv('FABRIC_TENANT_ID')
client_id = os.getenv('FABRIC_CLIENT_ID')
client_secret = os.getenv('FABRIC_CLIENT_SECRET')
workspace_id = os.getenv('FABRIC_WORKSPACE_ID')

# Get OAuth token
token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
token_data = {
    'grant_type': 'client_credentials',
    'client_id': client_id,
    'client_secret': client_secret,
    'scope': 'https://analysis.windows.net/powerbi/api/.default'
}
token_response = requests.post(token_url, data=token_data)
token_response.raise_for_status()
access_token = token_response.json()['access_token']

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

print("=" * 80)
print("COMPARING FABRIC MODELS: WORKING vs NOT WORKING")
print("=" * 80)

# Get all datasets
datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
response = requests.get(datasets_url, headers=headers)
response.raise_for_status()
datasets = response.json().get('value', [])

# Models to compare
models_to_check = {
    'working': ['annual', 'continent', 'industry', 'probablility'],  # These sync with columns
    'not_working': ['new_rep']  # This syncs as empty
}

all_model_info = []

print("\n" + "=" * 80)
print("ANALYZING ALL MODELS")
print("=" * 80)

for ds in datasets:
    name = ds['name']
    dataset_id = ds['id']
    
    # Determine if this is in our test set
    status = None
    if name.lower() in [m.lower() for m in models_to_check['working']]:
        status = "WORKING"
    elif name.lower() in [m.lower() for m in models_to_check['not_working']]:
        status = "NOT WORKING"
    else:
        status = "OTHER"
    
    print(f"\n{'='*80}")
    print(f"Model: {name} [{status}]")
    print(f"{'='*80}")
    print(f"  ID: {dataset_id}")
    print(f"  AddRowsAPIEnabled: {ds.get('addRowsAPIEnabled', False)}")
    print(f"  IsRefreshable: {ds.get('isRefreshable', 'N/A')}")
    print(f"  ConfiguredBy: {ds.get('configuredBy', 'N/A')}")
    print(f"  TargetStorageMode: {ds.get('targetStorageMode', 'N/A')}")
    
    # Try to get tables via REST API
    tables_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
    try:
        tables_response = requests.get(tables_url, headers=headers)
        if tables_response.status_code == 200:
            tables = tables_response.json().get('value', [])
            print(f"\n  REST API Tables: {len(tables)} table(s)")
            for table in tables:
                columns = table.get('columns', [])
                print(f"    - {table['name']}: {len(columns)} columns")
        elif tables_response.status_code == 404:
            error = tables_response.json().get('error', {})
            print(f"\n  REST API Tables: FAILED (404)")
            print(f"    Error: {error.get('message', 'Unknown')}")
        else:
            print(f"\n  REST API Tables: FAILED ({tables_response.status_code})")
    except Exception as e:
        print(f"\n  REST API Tables: ERROR - {e}")
    
    # Try to execute a DMV query (similar to what the sync tool does)
    dmv_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
    dmv_query = {
        "queries": [
            {
                "query": "EVALUATE TOPN(1, INFO.TABLES())"
            }
        ],
        "serializerSettings": {
            "includeNulls": True
        }
    }
    
    try:
        dmv_response = requests.post(dmv_url, json=dmv_query, headers=headers)
        if dmv_response.status_code == 200:
            print(f"\n  DMV Query: SUCCESS")
            results = dmv_response.json().get('results', [])
            if results and results[0].get('tables'):
                rows = results[0]['tables'][0].get('rows', [])
                print(f"    Can execute DMV queries: {len(rows)} sample row(s)")
        elif dmv_response.status_code == 400:
            error = dmv_response.json().get('error', {})
            print(f"\n  DMV Query: FAILED (400)")
            print(f"    Error: {error.get('code', 'Unknown')}")
            pbi_error = error.get('pbi.error', {})
            if pbi_error:
                details = pbi_error.get('details', [])
                for detail in details:
                    if detail.get('code') == 'DetailsMessage':
                        print(f"    Details: {detail.get('detail', {}).get('value', '')}")
        else:
            print(f"\n  DMV Query: FAILED ({dmv_response.status_code})")
    except Exception as e:
        print(f"\n  DMV Query: ERROR - {e}")

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print("\nKey Findings:")
print("  - WORKING models (annual, continent, etc.): Can access via REST API or DMV")
print("  - NOT WORKING models (new_rep): ???")
print("\nConclusion will be in the analysis above.")
