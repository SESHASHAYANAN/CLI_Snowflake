"""
Advanced Fabric to Snowflake Sync using XMLA/TOM

This script attempts to read ALL datasets (including standard ones)
using the XMLA endpoint and syncs them to Snowflake.

Requirements:
- Fabric Premium or PPU capacity
- XMLA Read enabled in tenant settings
"""

import sys
import os
import json

sys.path.append(os.getcwd())

from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient
import requests
import snowflake.connector


def map_ssas_type_to_snowflake(ssas_type):
    """Map SSAS/Tabular data types to Snowflake types."""
    type_map = {
        "string": "VARCHAR(500)",
        "int64": "INTEGER",
        "double": "FLOAT",
        "decimal": "DECIMAL(18, 4)",
        "boolean": "BOOLEAN",
        "dateTime": "TIMESTAMP",
        "date": "DATE",
        "binary": "BINARY",
    }
    return type_map.get(ssas_type.lower(), "VARCHAR(500)")


def map_ssas_type_to_powerbi(ssas_type):
    """Map SSAS types to Power BI types."""
    type_map = {
        "string": "String",
        "int64": "Int64",
        "double": "Double",
        "decimal": "Double",
        "boolean": "Boolean",
        "dateTime": "DateTime",
        "date": "DateTime",
    }
    return type_map.get(ssas_type.lower(), "String")


class AdvancedFabricSync:
    def __init__(self):
        self.settings = get_settings()
        self.fabric_config = self.settings.get_fabric_config()
        self.snowflake_config = self.settings.get_snowflake_config()
        self.workspace_id = self.fabric_config.workspace_id
        self.token = None
        self.sf_conn = None
        
    def authenticate(self):
        """Authenticate with Fabric."""
        print("Authenticating with Fabric...")
        oauth_client = FabricOAuthClient(config=self.fabric_config)
        self.token = oauth_client.get_access_token()
        print("[OK]")
        
    def connect_snowflake(self):
        """Connect to Snowflake."""
        print("Connecting to Snowflake...")
        self.sf_conn = snowflake.connector.connect(
            account=self.snowflake_config.account,
            user=self.snowflake_config.user,
            password=self.snowflake_config.password.get_secret_value(),
            warehouse=self.snowflake_config.warehouse,
            database=self.snowflake_config.database,
            schema=self.snowflake_config.schema_name
        )
        print("[OK]")
        
    def get_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
    
    def get_all_datasets(self):
        """Get all datasets from workspace."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets"
        response = requests.get(url, headers=self.get_headers())
        return response.json().get("value", [])
    
    def get_dataset_schema_via_dax(self, dataset_id, dataset_name):
        """
        Try to get dataset schema using DAX queries.
        Works for datasets that support DAX (most semantic models).
        """
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets/{dataset_id}/executeQueries"
        
        # Query for tables
        tables_query = {
            "queries": [{"query": "EVALUATE INFO.TABLES()"}],
            "serializerSettings": {"includeNulls": True}
        }
        
        response = requests.post(url, headers=self.get_headers(), json=tables_query)
        
        if response.status_code != 200:
            print(f"   [SKIP] Cannot query schema (Status: {response.status_code})")
            return None
        
        result = response.json()
        tables_data = result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
        
        tables = []
        for row in tables_data:
            table_name = row.get("[Name]", "")
            if table_name and not table_name.startswith("$") and not table_name.startswith("DateTableTemplate"):
                tables.append({"name": table_name, "id": row.get("[ID]", "")})
        
        if not tables:
            print(f"   [SKIP] No user tables found")
            return None
        
        # For each table, get columns
        schema = []
        for table in tables:
            table_name = table["name"]
            
            # Query for columns of this table
            columns_query = {
                "queries": [{"query": f"EVALUATE INFO.COLUMNS()"}],
                "serializerSettings": {"includeNulls": True}
            }
            
            col_response = requests.post(url, headers=self.get_headers(), json=columns_query)
            
            if col_response.status_code == 200:
                col_result = col_response.json()
                cols_data = col_result.get("results", [{}])[0].get("tables", [{}])[0].get("rows", [])
                
                columns = []
                for col in cols_data:
                    col_table_id = col.get("[TableID]", "")
                    if str(col_table_id) == str(table.get("id", "")):
                        col_name = col.get("[ExplicitName]") or col.get("[InferredName]", "Unknown")
                        col_type = col.get("[ExplicitDataType]") or col.get("[InferredDataType]", "String")
                        
                        if col_name and not col_name.startswith("RowNumber"):
                            columns.append({
                                "name": col_name,
                                "type": col_type
                            })
                
                if columns:
                    schema.append({
                        "table": table_name,
                        "columns": columns
                    })
                    print(f"   - {table_name}: {len(columns)} columns")
        
        return schema if schema else None
    
    def create_snowflake_table(self, table_name, columns):
        """Create a table in Snowflake."""
        cursor = self.sf_conn.cursor()
        schema = self.snowflake_config.schema_name
        db = self.snowflake_config.database
        
        col_defs = []
        for col in columns:
            sf_type = map_ssas_type_to_snowflake(col["type"])
            # Clean column name - remove special characters
            clean_name = col["name"].replace(" ", "_").replace("-", "_")
            col_defs.append(f'"{clean_name}" {sf_type}')
        
        sql = f'CREATE TABLE IF NOT EXISTS {db}.{schema}."{table_name}" ({", ".join(col_defs)})'
        
        try:
            cursor.execute(sql)
            return True
        except Exception as e:
            print(f"   [ERROR] {e}")
            return False
        finally:
            cursor.close()
    
    def create_push_dataset(self, name, tables_schema):
        """Create a Push dataset in Fabric."""
        url = f"https://api.powerbi.com/v1.0/myorg/groups/{self.workspace_id}/datasets"
        
        tables_def = []
        for table in tables_schema:
            columns_def = []
            for col in table["columns"]:
                pb_type = map_ssas_type_to_powerbi(col["type"])
                clean_name = col["name"].replace(" ", "_").replace("-", "_")
                columns_def.append({"name": clean_name, "dataType": pb_type})
            
            tables_def.append({
                "name": table["table"],
                "columns": columns_def
            })
        
        payload = {
            "name": name,
            "defaultMode": "Push",
            "tables": tables_def
        }
        
        response = requests.post(url, headers=self.get_headers(), json=payload)
        
        if response.status_code in [200, 201]:
            return response.json().get("id")
        elif response.status_code == 409:
            # Already exists - find its ID
            datasets = self.get_all_datasets()
            for ds in datasets:
                if ds["name"] == name:
                    return ds["id"]
        return None
    
    def sync_all(self):
        """Main sync function."""
        print("=" * 70)
        print("ADVANCED FABRIC -> SNOWFLAKE SYNC")
        print("=" * 70)
        print()
        
        self.authenticate()
        self.connect_snowflake()
        
        print()
        print("Discovering datasets...")
        datasets = self.get_all_datasets()
        print(f"Found {len(datasets)} datasets")
        print()
        
        synced_schemas = []
        
        for ds in datasets:
            ds_id = ds["id"]
            ds_name = ds["name"]
            
            # Skip already synced Push datasets
            if ds_name in ["SnowflakeSync", "SnowflakeComplete"]:
                print(f"[SKIP] {ds_name} (already a sync dataset)")
                continue
            
            print(f"[SYNC] {ds_name}")
            
            # Try to get schema via DAX
            schema = self.get_dataset_schema_via_dax(ds_id, ds_name)
            
            if schema:
                # Create tables in Snowflake
                for table_info in schema:
                    table_name = f"{ds_name}_{table_info['table']}"
                    self.create_snowflake_table(table_name, table_info["columns"])
                
                synced_schemas.extend([{
                    "dataset": ds_name,
                    "table": f"{ds_name}_{t['table']}",
                    "columns": t["columns"]
                } for t in schema])
            
            print()
        
        # Update semantic view
        if synced_schemas:
            print("Updating Snowflake Semantic View...")
            cursor = self.sf_conn.cursor()
            schema = self.snowflake_config.schema_name
            db = self.snowflake_config.database
            
            cursor.execute(f"""
                CREATE OR REPLACE VIEW {db}.{schema}.SEMANTIC_VIEW AS
                SELECT table_name, column_name, data_type
                FROM {db}.information_schema.columns
                WHERE table_schema = '{schema}'
            """)
            cursor.close()
            print("[OK]")
        
        # Close connections
        if self.sf_conn:
            self.sf_conn.close()
        
        print()
        print("=" * 70)
        print("SYNC COMPLETE")
        print("=" * 70)
        
        if synced_schemas:
            print(f"Synced {len(synced_schemas)} tables to Snowflake")
        else:
            print("No new tables were synced.")
            print()
            print("NOTE: Standard datasets may require XMLA endpoint access.")
            print("      Enable XMLA Read in your Fabric Admin Portal:")
            print("      Settings > Admin Portal > Integration settings > XMLA")


def main():
    syncer = AdvancedFabricSync()
    syncer.sync_all()


if __name__ == "__main__":
    main()
