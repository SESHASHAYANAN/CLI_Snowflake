import snowflake.connector
from dotenv import load_dotenv
import os

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

print("="*60)
print("SNOWFLAKE: Checking for ANNUAL model metadata")
print("="*60)

cursor.execute("SELECT MODEL_NAME, SYNC_VERSION, SYNCED_AT FROM _SEMANTIC_METADATA WHERE MODEL_NAME = 'annual' ORDER BY SYNC_VERSION DESC LIMIT 1")
row = cursor.fetchone()
if row:
    print(f"Model: {row[0]}")
    print(f"Version: {row[1]}")
    print(f"Synced At: {row[2]}")
    print()
    print("[OK] Annual model found in Snowflake!")
else:
    print("[WARN] Annual model not found")

cursor.close()
conn.close()
