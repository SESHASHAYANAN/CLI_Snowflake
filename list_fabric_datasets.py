"""List all Fabric datasets"""
from msal import ConfidentialClientApplication
from dotenv import load_dotenv
import requests
import os

load_dotenv()

# Auth
app = ConfidentialClientApplication(
    os.getenv("FABRIC_CLIENT_ID"),
    authority=f"https://login.microsoftonline.com/{os.getenv('FABRIC_TENANT_ID')}",
    client_credential=os.getenv("FABRIC_CLIENT_SECRET")
)
token = app.acquire_token_for_client(scopes=["https://analysis.windows.net/powerbi/api/.default"])["access_token"]

# List datasets
workspace_id = os.getenv("FABRIC_WORKSPACE_ID")
url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
headers = {"Authorization": f"Bearer {token}"}
response = requests.get(url, headers=headers, timeout=30)
datasets = response.json().get("value", [])

print("="*70)
print("ALL DATASETS IN FABRIC WORKSPACE")
print("="*70)

for ds in sorted(datasets, key=lambda x: x.get("name", "")):
    name = ds.get("name", "Unknown")
    ds_id = ds.get("id", "Unknown")
    marker = " <-- NEW_REP" if "new_rep" in name.lower() else ""
    print(f"{name:45} {ds_id}{marker}")

print(f"\nTotal: {len(datasets)} datasets")
