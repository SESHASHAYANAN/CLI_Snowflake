
import sys
import os
sys.path.append(os.getcwd())
from semantic_sync.config import get_settings
import snowflake.connector

def create_table():
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
    
    try:
        # 1. Create Table
        table_name = "DEMO_PRODUCTS"
        print(f"\nCreating table {table_name}...")
        cursor.execute(f"""
            CREATE OR REPLACE TABLE {conf.schema_name}.{table_name} (
                PRODUCT_ID INT,
                PRODUCT_NAME VARCHAR(100),
                CATEGORY VARCHAR(50),
                PRICE DECIMAL(10, 2)
            )
        """)
        
        # 2. Insert Data
        print("Inserting sample data...")
        cursor.execute(f"""
            INSERT INTO {conf.schema_name}.{table_name} VALUES 
            (1, 'Super Widget', 'Widgets', 19.99),
            (2, 'Mega Gadget', 'Gadgets', 49.99),
            (3, 'Ultra Thingy', 'Widgets', 29.50)
        """)
        
        print(f"[OK] Table {table_name} created and populated.")
        
    except Exception as e:
        print(f"[ERROR] {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    create_table()
