
import sys
import os
sys.path.append(os.getcwd())
from semantic_sync.config import get_settings
import snowflake.connector

def check_structure():
    settings = get_settings()
    conf = settings.get_snowflake_config()
    
    print(f"Connecting to {conf.database}.{conf.schema_name}...")
    conn = snowflake.connector.connect(
        account=conf.account,
        user=conf.user,
        password=conf.password.get_secret_value(),
        warehouse=conf.warehouse,
        database=conf.database,
        schema=conf.schema_name
    )
    cursor = conn.cursor()
    
    # 1. List Base Tables
    print("\n[BASE TABLES]")
    cursor.execute(f"SHOW TABLES IN SCHEMA {conf.schema_name}")
    for row in cursor.fetchall():
        # row[1] is name
        print(f" - {row[1]}")
        
    # 2. List Views
    print("\n[VIEWS]")
    cursor.execute(f"SHOW VIEWS IN SCHEMA {conf.schema_name}")
    for row in cursor.fetchall():
        print(f" - {row[1]}")
        
    # 3. Check Metadata Table Content
    print("\n[SYNCED MODELS in _SEMANTIC_METADATA]")
    try:
        cursor.execute(f"SELECT MODEL_NAME, TABLE_COUNT, MEASURE_COUNT FROM {conf.schema_name}._SEMANTIC_METADATA")
        for row in cursor.fetchall():
            print(f" - Model: {row[0]:<20} | Tables: {row[1]} | Measures: {row[2]}")
    except Exception as e:
        print(f"Error reading metadata: {e}")

    conn.close()

if __name__ == "__main__":
    check_structure()
