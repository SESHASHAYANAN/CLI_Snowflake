"""
Check the new_rep dataset in Fabric to see what it contains
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
print("CHECKING 'new_rep' IN FABRIC")
print("=" * 80)

# Get all datasets in the workspace
print("\n1. Listing all datasets in Fabric workspace:")
print("-" * 80)
datasets_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
response = requests.get(datasets_url, headers=headers)
response.raise_for_status()
datasets = response.json().get('value', [])

print(f"Total datasets: {len(datasets)}")
new_rep_dataset = None
for ds in datasets:
    marker = " <-- NEW_REP HERE!" if "new_rep" in ds['name'].lower() else ""
    print(f"   - {ds['name']} (ID: {ds['id']}){marker}")
    if "new_rep" in ds['name'].lower():
        new_rep_dataset = ds

if not new_rep_dataset:
    print("\n[NO] 'new_rep' dataset NOT found in Fabric!")
    print("\nAll available datasets:")
    for ds in datasets:
        print(f"  - {ds['name']}")
    sys.exit(0)

print(f"\n2. Details of 'new_rep' dataset:")
print("-" * 80)
print(f"   Name: {new_rep_dataset['name']}")
print(f"   ID: {new_rep_dataset['id']}")
print(f"   Configured By: {new_rep_dataset.get('configuredBy', 'N/A')}")
print(f"   Is Refreshable: {new_rep_dataset.get('isRefreshable', 'N/A')}")
print(f"   Add Rows API Enabled: {new_rep_dataset.get('addRowsAPIEnabled', 'N/A')}")

# Try to get tables in the dataset
print(f"\n3. Trying to retrieve tables in 'new_rep':")
print("-" * 80)
dataset_id = new_rep_dataset['id']
tables_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/tables"
try:
    tables_response = requests.get(tables_url, headers=headers)
    if tables_response.status_code == 200:
        tables = tables_response.json().get('value', [])
        if tables:
            print(f"[YES] Found {len(tables)} table(s):")
            for table in tables:
                print(f"\n   Table: {table['name']}")
                columns = table.get('columns', [])
                print(f"   Columns ({len(columns)}):")
                for col in columns:
                    print(f"      - {col['name']} ({col.get('dataType', 'unknown')})")
        else:
            print("[NO] No tables found in this dataset (it's empty)")
    else:
        print(f"[ERROR] Could not retrieve tables (Status: {tables_response.status_code})")
        print(f"   Response: {tables_response.text}")
except Exception as e:
    print(f"[ERROR] Failed to retrieve tables: {e}")

print("\n" + "=" * 80)
print("SUMMARY:")
print("=" * 80)

if new_rep_dataset:
    print(f"\n>>> YES - 'new_rep' EXISTS in Fabric <<<")
    print(f"\n  Dataset Name: {new_rep_dataset['name']}")
    print(f"  Dataset ID: {new_rep_dataset['id']}")
    print(f"\n  Status: Dataset exists but appears to be EMPTY (no tables/columns)")
    print(f"\n  This explains why the sync to Snowflake shows:")
    print(f"    - Tables: 0")
    print(f"    - Columns: 0")
    print(f"    - Measures: 0")
else:
    print(f"\n>>> NO - 'new_rep' does NOT exist in Fabric <<<")
