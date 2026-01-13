"""
Create the Sales Analytics sample dataset in Fabric.
"""
import sys
import os
import json
import requests
from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient

# Import sample data structure
sys.path.insert(0, ".")
from tests.fixtures.sample_fabric_data import create_sales_model

def map_type(dtype):
    """Map generic types to Power BI Push API types."""
    dtype = dtype.lower()
    if 'int' in dtype: return 'Int64'
    if 'float' in dtype or 'double' in dtype or 'decimal' in dtype: return 'Double'
    if 'bool' in dtype: return 'Boolean'
    if 'date' in dtype: return 'DateTime'
    return 'String'

def main():
    print("Creating 'SalesAnalytics' dataset in Fabric...")
    
    # Load config and auth
    settings = get_settings()
    fabric_config = settings.get_fabric_config()
    oauth_client = FabricOAuthClient(config=fabric_config)
    token = oauth_client.get_access_token()
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    # Get sample model structure
    model = create_sales_model()
    
    # Build Push Dataset tables definition
    tables_def = []
    for table in model.tables:
        cols_def = []
        for col in table.columns:
            cols_def.append({
                "name": col.name,
                "dataType": map_type(col.data_type)
            })
        
        tables_def.append({
            "name": table.name,
            "columns": cols_def
        })
        
    dataset_definition = {
        "name": "SalesAnalytics",
        "defaultMode": "Push",
        "tables": tables_def
    }
    
    # Create
    workspace_id = fabric_config.workspace_id
    url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
    
    print(f"Sending request to create dataset with {len(tables_def)} tables...")
    resp = requests.post(url, headers=headers, json=dataset_definition)
    
    if resp.status_code in [200, 201]:
        data = resp.json()
        new_id = data['id']
        print(f"SUCCESS! Dataset created with ID: {new_id}")
        
        # Update .env
        env_path = os.path.join(os.getcwd(), '.env')
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                lines = f.readlines()
            
            with open(env_path, 'w') as f:
                found = False
                for line in lines:
                    if line.startswith("FABRIC_DATASET_ID="):
                        f.write(f"FABRIC_DATASET_ID={new_id}\n")
                        found = True
                    else:
                        f.write(line)
                if not found:
                    f.write(f"\nFABRIC_DATASET_ID={new_id}\n")
            print("Updated .env with new FABRIC_DATASET_ID.")
            
    else:
        print(f"FAILED: {resp.status_code}")
        print(resp.text)

if __name__ == "__main__":
    main()
