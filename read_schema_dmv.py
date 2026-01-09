"""
Alternative script to read schema from a non-Push dataset using DMV (Dynamic Management Views)
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
    """Read schema using DMV queries."""
    print("="*60)
    print("Dataset Schema Reader (DMV Method)")
    print("="*60)
    print()
    
    try:
        # Load configuration
        settings = get_settings()
        fabric_config = settings.get_fabric_config()
        
        workspace_id = fabric_config.workspace_id
        dataset_id = fabric_config.dataset_id
        
        print(f"Workspace ID: {workspace_id}")
        print(f"Dataset ID: {dataset_id}")
        print()
        
        # Authenticate
        print("Authenticating...")
        oauth_client = FabricOAuthClient(config=fabric_config)
        token = oauth_client.get_access_token()
        print("[OK] Authentication successful!")
        print()
        
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        # Use executeQueries endpoint with DMV query
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{dataset_id}/executeQueries"
        
        # DMV query to get tables
        print("Querying for tables...")
        tables_query = {
            "queries": [
                {
                    "query": "SELECT [Name], [Description], [IsHidden] FROM $SYSTEM.TMSCHEMA_TABLES WHERE [ObjectType] = 'Table'"
                }
            ],
            "serializerSettings": {
                "includeNulls": False
            }
        }
        
        response = requests.post(url, headers=headers, json=tables_query)
        
        if response.status_code != 200:
            print(f"[ERROR] Failed to query tables")
            print(f"Status: {response.status_code}")
            print(f"Response: {response.text}")
            return
        
        tables_result = response.json()
        tables_data = tables_result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
        
        print(f"[OK] Found {len(tables_data)} table(s)")
        print()
        
        # For each table, get columns
        for table_row in tables_data:
            table_name = table_row.get("[Name]", "Unknown")
            table_desc = table_row.get("[Description]", "")
            is_hidden = table_row.get("[IsHidden]", False)
            
            print("="*60)
            print(f"Table: {table_name}")
            if table_desc:
                print(f"Description: {table_desc}")
            print(f"Hidden: {is_hidden}")
            print("-"*60)
            
            # Query columns for this table
            columns_query = {
                "queries": [
                    {
                        "query": f"SELECT [Name], [DataType], [IsHidden], [Description] FROM $SYSTEM.TMSCHEMA_COLUMNS WHERE [TableName] = '{table_name}'"
                    }
                ],
                "serializerSettings": {
                    "includeNulls": False
                }
            }
            
            cols_response = requests.post(url, headers=headers, json=columns_query)
            
            if cols_response.status_code == 200:
                cols_result = cols_response.json()
                cols_data = cols_result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                
                print(f"Columns ({len(cols_data)}):")
                for col_row in cols_data:
                    col_name = col_row.get("[Name]", "Unknown")
                    col_type = col_row.get("[DataType]", "Unknown")
                    col_hidden = col_row.get("[IsHidden]", False)
                    col_desc = col_row.get("[Description]", "")
                    
                    hidden_mark = " [HIDDEN]" if col_hidden else ""
                    print(f"  - {col_name} ({col_type}){hidden_mark}")
                    if col_desc:
                        print(f"      Description: {col_desc}")
            else:
                print(f"  [ERROR] Failed to query columns")
                print(f"  Status: {cols_response.status_code}")
            
            print()
        
        # Query measures
        print("="*60)
        print("Querying for measures...")
        measures_query = {
            "queries": [
                {
                    "query": "SELECT [Name], [Expression], [Description], [IsHidden] FROM $SYSTEM.TMSCHEMA_MEASURES"
                }
            ],
            "serializerSettings": {
                "includeNulls": False
            }
        }
        
        measures_response = requests.post(url, headers=headers, json=measures_query)
        
        if measures_response.status_code == 200:
            measures_result = measures_response.json()
            measures_data = measures_result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
            
            print(f"[OK] Found {len(measures_data)} measure(s)")
            print()
            
            for measure_row in measures_data:
                measure_name = measure_row.get("[Name]", "Unknown")
                measure_expr = measure_row.get("[Expression]", "")
                measure_desc = measure_row.get("[Description]", "")
                measure_hidden = measure_row.get("[IsHidden]", False)
                
                hidden_mark = " [HIDDEN]" if measure_hidden else ""
                print(f"Measure: {measure_name}{hidden_mark}")
                if measure_desc:
                    print(f"  Description: {measure_desc}")
                if measure_expr:
                    print(f"  Expression: {measure_expr[:100]}...")
                print()
        else:
            print(f"[ERROR] Failed to query measures")
            print(f"Status: {measures_response.status_code}")
            print(f"Response: {measures_response.text}")
        
        print("="*60)
        print("[OK] Schema reading completed!")
        print("="*60)
        
    except Exception as e:
        print(f"[ERROR] {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
