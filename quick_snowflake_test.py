"""Quick test: Add column to Snowflake and sync to Fabric"""
import snowflake.connector
from dotenv import load_dotenv
import os
import sys

load_dotenv()

print("="*60)
print("STEP 1: Adding new column in Snowflake")
print("="*60)

conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA")
)
cursor = conn.cursor()

try:
    cursor.execute("ALTER TABLE PRODUCTS ADD COLUMN DISCOUNT_PERCENT DECIMAL(5,2)")
    print("[OK] Added column: DISCOUNT_PERCENT (DECIMAL)")
    
    cursor.execute("UPDATE PRODUCTS SET DISCOUNT_PERCENT = 10.5 WHERE ProductID = 1")
    cursor.execute("UPDATE PRODUCTS SET DISCOUNT_PERCENT = 15.0 WHERE ProductID = 2")
    print("[OK] Populated sample data")
except Exception as e:
    if "already exists" in str(e):
        print("[INFO] Column DISCOUNT_PERCENT already exists")
    else:
        print(f"[ERROR] {e}")
finally:
    cursor.close()
    conn.close()

print()
print("="*60)
print("STEP 2: Syncing to Fabric...")
print("="*60)

from semantic_sync.main import main as cli_main
from unittest.mock import patch

with patch.object(sys, "argv", ["semantic-sync", "sf-to-fabric", "--mode", "incremental"]):
    try:
        cli_main()
    except SystemExit:
        pass

print()
print("="*60)
print("VERIFICATION")
print("="*60)
print("Check Fabric UI for the newest 'SnowflakeSync_*' dataset.")
print("Look for column: DISCOUNT_PERCENT in the Products table.")
