"""
Create a Push API Dataset in Fabric for Snowflake Sync
This will create a dataset that supports the REST API /tables endpoint
"""

import sys
import os
import json

# Set UTF-8 encoding for Windows console
if sys.platform == "win32":
    os.system("chcp 65001 >nul 2>&1")

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests

def main():
    """Create a Push API dataset in Fabric."""
    print("="*80)
    print("CREATE PUSH API DATASET FOR SNOWFLAKE SYNC")
    print("="*80)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        snowflake_config = settings.get_snowflake_config()
        
        workspace_id = fabric_config.workspace_id
        
        # Authenticate
        print("Authenticating with Microsoft Fabric...")
        oauth_client = FabricOAuthClient(config=fabric_config)
        token = oauth_client.get_access_token()
        print("[OK] Authentication successful!")
        print()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Define the new Push dataset
        dataset_name = "SnowflakeSync"
        print(f"Creating Push dataset: '{dataset_name}'")
        print()
        
        # Create dataset with sample table structure
        # We'll use a simple schema that can be expanded later
        dataset_definition = {
            "name": dataset_name,
            "defaultMode": "Push",
            "tables": [
                {
                    "name": "SnowflakeData",
                    "columns": [
                        {
                            "name": "TableName",
                            "dataType": "String"
                        },
                        {
                            "name": "ColumnName",
                            "dataType": "String"
                        },
                        {
                            "name": "DataType",
                            "dataType": "String"
                        },
                        {
                            "name": "IsNullable",
                            "dataType": "Boolean"
                        },
                        {
                            "name": "LastSyncedAt",
                            "dataType": "DateTime"
                        }
                    ]
                }
            ]
        }
        
        # Create the dataset
        create_url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets"
        
        print("Sending request to create dataset...")
        print(f"Dataset definition:")
        print(json.dumps(dataset_definition, indent=2))
        print()
        
        response = requests.post(create_url, headers=headers, json=dataset_definition)
        
        if response.status_code in [200, 201]:
            result = response.json()
            new_dataset_id = result.get('id')
            
            print("="*80)
            print("SUCCESS! Push Dataset Created")
            print("="*80)
            print(f"Dataset Name: {dataset_name}")
            print(f"Dataset ID: {new_dataset_id}")
            print()
            print("This dataset:")
            print("  - Is a Push API dataset")
            print("  - Supports REST API /tables endpoint")
            print("  - Can have schema modified via API")
            print("  - Works with semantic-sync tool")
            print()
            
            # Update .env file suggestion
            env_path = os.path.join(os.getcwd(), '.env')
            print("="*80)
            print("NEXT STEPS")
            print("="*80)
            print()
            print("1. Update your .env file with the new dataset ID:")
            print(f"   FABRIC_DATASET_ID={new_dataset_id}")
            print()
            print("2. Then run the sync:")
            print("   python -m semantic_sync.main sync --direction snowflake-to-fabric --dry-run")
            print()
            
            # Optionally update .env automatically
            print("Would you like me to update your .env file automatically? (y/n)")
            # For automation, let's create an update script
            
            update_env_script = f"""
# To update .env automatically, run:
# 
# In PowerShell:
(Get-Content .env) -replace 'FABRIC_DATASET_ID=.*', 'FABRIC_DATASET_ID={new_dataset_id}' | Set-Content .env
#
# Or manually edit .env and change line:
# FABRIC_DATASET_ID={new_dataset_id}
"""
            
            with open('update_dataset_id.txt', 'w') as f:
                f.write(update_env_script)
            
            print()
            print("[INFO] Instructions saved to update_dataset_id.txt")
            print()
            
        elif response.status_code == 409:
            print("[WARNING] A dataset with this name already exists!")
            print("Response:", response.text)
            print()
            print("Options:")
            print("1. Delete the existing dataset and try again")
            print("2. Use a different name")
            print("3. Use the existing Push dataset if it has one")
            
        else:
            print(f"[ERROR] Failed to create dataset")
            print(f"Status Code: {response.status_code}")
            print(f"Response: {response.text}")
            print()
            
            # Try to provide helpful error messages
            if "Premium" in response.text or "capacity" in response.text.lower():
                print("[INFO] Push datasets may require Premium capacity in some cases.")
                print("      However, basic Push datasets should work in Pro workspaces.")
            
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
