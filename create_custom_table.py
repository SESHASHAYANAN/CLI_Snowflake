
import sys
import os
import snowflake.connector

# Add project root to path for config loading
sys.path.append(os.getcwd())
from semantic_sync.config import get_settings

def create_custom_table():
    # --- [EDIT THIS SECTION] ---
    TABLE_NAME = "MY_NEW_TABLE"
    
    # Define your columns here (Name, Type)
    COLUMNS = [
        ("ID", "INT"),
        ("NAME", "VARCHAR(100)"),
        ("DATE_ADDED", "DATE"),
        ("IS_ACTIVE", "BOOLEAN")
    ]
    
    # Optional: Add some data?
    INSERT_DATA = [
        (1, "Item A", "2024-01-01", True),
        (2, "Item B", "2024-01-02", False)
    ]
    # ---------------------------

    print(f"preparing to create {TABLE_NAME}...")
    
    try:
        settings = get_settings()
        conf = settings.get_snowflake_config()
        
        conn = snowflake.connector.connect(
            account=conf.account,
            user=conf.user,
            password=conf.password.get_secret_value(),
            warehouse=conf.warehouse,
            database=conf.database,
            schema=conf.schema_name
        )
        cursor = conn.cursor()
        
        # Build SQL
        cols_sql = ",\n".join([f"{name} {dtype}" for name, dtype in COLUMNS])
        create_sql = f"CREATE OR REPLACE TABLE {conf.schema_name}.{TABLE_NAME} (\n{cols_sql}\n)"
        
        print(f"Executing:\n{create_sql}")
        cursor.execute(create_sql)
        print("Table created successfully.")
        
        if INSERT_DATA:
            placeholders = ", ".join(["%s"] * len(COLUMNS))
            insert_sql = f"INSERT INTO {conf.schema_name}.{TABLE_NAME} VALUES ({placeholders})"
            print(f"Inserting {len(INSERT_DATA)} rows...")
            cursor.executemany(insert_sql, INSERT_DATA)
            print("Data inserted.")

        conn.close()
        print("\nDone! run 'semantic-sync sf-to-fabric' to sync this table.")

    except Exception as e:
        print(f"[ERROR] {e}")

if __name__ == "__main__":
    create_custom_table()
