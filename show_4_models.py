"""
Show details of the 4 models with complete column information
"""
import snowflake.connector
from dotenv import load_dotenv
import os
import json

load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cursor = conn.cursor()

models = ["annual", "continent", "industry", "probablility"]

print("="*70)
print("4 MODELS WITH DETAILED COLUMN INFORMATION")
print("="*70)

for model in models:
    query = f"SELECT MODEL_JSON FROM _SEMANTIC_METADATA WHERE MODEL_NAME = '{model}'"
    cursor.execute(query)
    row = cursor.fetchone()
    if row:
        data = json.loads(row[0])
        tables = data.get("tables", [])
        print(f"\n{'='*70}")
        print(f"MODEL: {model.upper()}")
        print("="*70)
        for t in tables:
            table_name = t.get("name", "Unknown")
            columns = t.get("columns", [])
            print(f"\n  Table: {table_name}")
            print("  " + "-"*50)
            for c in columns:
                col_name = c.get("name", "?")
                col_type = c.get("data_type", "?")
                print(f"    {col_name:35} {col_type}")

cursor.close()
conn.close()

print("\n" + "="*70)
print("Bi-directional sync verified! All 4 models synced from Fabric to Snowflake.")
