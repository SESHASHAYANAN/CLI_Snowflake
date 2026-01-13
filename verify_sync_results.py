"""
Verify the content of a Fabric Dataset by querying TMSCHEMA_COLUMNS via DAX.
This is more reliable than the standard /tables endpoint for some dataset types.
"""
import sys
import os
import requests
import json
from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient

def main():
    print("="*60)
    print("VERIFY SYNC RESULTS: Dataset Columns (DMV Method)")
    print("="*60)
    
    settings = get_settings()
    fabric_config = settings.get_fabric_config()
    
    print("Authenticating...")
    oauth = FabricOAuthClient(fabric_config)
    token = oauth.get_access_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    workspace_id = fabric_config.workspace_id
    
    # Get Datasets
    resp = requests.get(f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets", headers=headers)
    datasets = resp.json().get("value", [])
    
    targets = [d for d in datasets if "SnowflakeSync" in d['name'] or "SalesAnalytics" in d['name']]
    
    if not targets:
        print("No target datasets found.")
        return

    # Sort by name (newest likely last if timed)
    targets.sort(key=lambda x: x['name'], reverse=True)
    
    for ds in targets[:3]: # Check top 3 newest
        print(f"\nScanning: {ds['name']}")
        
        # DAX Query to get Table and Column names
        dax_query = {
            "queries": [
                {
                    "query": """
                        SELECT 
                            [Name] AS [ColumnName],
                            [TableID]
                        FROM $SYSTEM.TMSCHEMA_COLUMNS
                    """
                },
                {
                    "query": """
                        SELECT 
                            [ID] AS [TableID],
                            [Name] AS [TableName]
                        FROM $SYSTEM.TMSCHEMA_TABLES
                    """
                }
            ],
            "serializerSettings": {"incudeNulls": True}
        }
        
        url_query = f"https://api.powerbi.com/v1.0/myorg/groups/{workspace_id}/datasets/{ds['id']}/executeQueries"
        q_resp = requests.post(url_query, headers=headers, json=dax_query)
        
        if q_resp.status_code != 200:
            print(f"  [Error] Query failed: {q_resp.status_code}")
            # print(q_resp.text)
            continue
            
        try:
            results = q_resp.json()
            # Parse Tables
            table_rows = results['results'][1]['tables'][0]['rows']
            table_map = {r['[TableID]']: r['[TableName]'] for r in table_rows}
            
            # Parse Columns
            col_rows = results['results'][0]['tables'][0]['rows']
            
            found_promo = False
            
            # Print structure
            print("  Structure:")
            
            # Group by table
            schema = {}
            for c in col_rows:
                tid = c['[TableID]']
                cname = c['[ColumnName]']
                tname = table_map.get(tid, "Unknown")
                
                if tname not in schema: schema[tname] = []
                schema[tname].append(cname)
                
                if tname.upper() == "PRODUCTS" and cname.upper() == "PROMO_TIER":
                    found_promo = True

            for tname, cols in schema.items():
                print(f"    Table: {tname}")
                print(f"      Columns: {', '.join(cols)}")
                
            if found_promo:
                print(f"\n  [SUCCESS] 'PROMO_TIER' Found in {ds['name']}!")
            else:
                print(f"\n  [INFO] 'PROMO_TIER' NOT found in {ds['name']}.")
                
        except Exception as e:
            print(f"  [Error] Parsing failed: {e}")
            # print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()
