"""
Check details of the 'annual' model synced to Snowflake
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

print("="*70)
print("ANNUAL MODEL DETAILS IN SNOWFLAKE")
print("="*70)

# 1. Get basic metadata
print("\n[1] BASIC METADATA")
print("-"*70)
cursor.execute("SELECT MODEL_NAME, SYNC_VERSION, SOURCE_SYSTEM, MODEL_JSON FROM _SEMANTIC_METADATA WHERE MODEL_NAME = 'annual'")
row = cursor.fetchone()
if row:
    print(f"  Model Name:    {row[0]}")
    print(f"  Sync Version:  {row[1]}")
    print(f"  Source System: {row[2]}")
    
    # Parse JSON to show tables and columns
    model_json = json.loads(row[3])
    tables = model_json.get("tables", [])
    
    print(f"\n[2] TABLES ({len(tables)} total)")
    print("-"*70)
    for table in tables:
        table_name = table.get("name", "Unknown")
        columns = table.get("columns", [])
        print(f"\n  Table: {table_name}")
        print(f"  Columns ({len(columns)}):")
        for col in columns:
            col_name = col.get("name", "Unknown")
            col_type = col.get("data_type", "Unknown")
            print(f"    - {col_name:30} ({col_type})")
    
    # Show measures
    measures = model_json.get("measures", [])
    if measures:
        print(f"\n[3] MEASURES ({len(measures)} total)")
        print("-"*70)
        for m in measures:
            print(f"  - {m.get('name', 'Unknown')}")
            if m.get("expression"):
                print(f"    Expression: {m.get('expression')[:50]}...")
    
    # Show relationships
    relationships = model_json.get("relationships", [])
    if relationships:
        print(f"\n[4] RELATIONSHIPS ({len(relationships)} total)")
        print("-"*70)
        for r in relationships:
            print(f"  - {r.get('from_table', '?')}.{r.get('from_column', '?')} -> {r.get('to_table', '?')}.{r.get('to_column', '?')}")
else:
    print("  [WARN] 'annual' model not found in metadata")

cursor.close()
conn.close()

print("\n" + "="*70)
print("Done!")
