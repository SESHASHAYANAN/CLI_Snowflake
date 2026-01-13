"""
Show all models and their tables/columns in Snowflake
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
print("ALL MODELS AND THEIR TABLES IN SNOWFLAKE")
print("="*70)

cursor.execute("SELECT MODEL_NAME, MODEL_JSON FROM _SEMANTIC_METADATA ORDER BY MODEL_NAME")
for row in cursor.fetchall():
    model_name = row[0]
    model_json = json.loads(row[1])
    tables = model_json.get("tables", [])
    
    print(f"\n{'='*70}")
    print(f"MODEL: {model_name}")
    print(f"{'='*70}")
    
    for table in tables:
        table_name = table.get("name", "Unknown")
        columns = table.get("columns", [])
        print(f"\n  TABLE: {table_name} ({len(columns)} columns)")
        print("  " + "-"*50)
        for col in columns:
            col_name = col.get("name", "?")
            col_type = col.get("data_type", "?")
            print(f"    {col_name:35} {col_type}")

cursor.close()
conn.close()
print("\n" + "="*70)
print("Done!")
