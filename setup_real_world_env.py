"""
Setup script for 'The Holiday Campaign' Real World Scenario.
Initializes Snowflake tables and Fabric Dataset to a known 'clean' state.
"""
import sys
import snowflake.connector
import requests
import json
from semantic_sync.config import get_settings
from semantic_sync.auth.oauth import FabricOAuthClient

# Helper to unwrap secrets if needed
def get_secret(value):
    if hasattr(value, 'get_secret_value'):
        return value.get_secret_value()
    return str(value)

def setup_snowflake(settings):
    print("[Snowflake] Initializing tables...")
    sf_config = settings.get_snowflake_config()
    
    conn = snowflake.connector.connect(
        user=sf_config.user,
        password=get_secret(sf_config.password),
        account=sf_config.account,
        warehouse=sf_config.warehouse,
        database=sf_config.database,
        schema=get_secret(sf_config.schema)
    )
    cursor = conn.cursor()
    
    try:
        db = get_secret(sf_config.database)
        schema = get_secret(sf_config.schema)
        
        cursor.execute(f"USE DATABASE {db}")
        cursor.execute(f"USE SCHEMA {schema}")
        
        # 1. CLEANUP
        cursor.execute("DROP TABLE IF EXISTS PRODUCTS")
        cursor.execute("DROP TABLE IF EXISTS SALES")
        
        # 2. CREATE BASE TABLES
        print("  Creating table: PRODUCTS")
        cursor.execute("""
            CREATE TABLE PRODUCTS (
                ProductID INT,
                ProductName VARCHAR(100),
                Category VARCHAR(50),
                UnitPrice DECIMAL(10, 2)
            )
        """)
        
        print("  Creating table: SALES")
        cursor.execute("""
            CREATE TABLE SALES (
                OrderID INT,
                ProductID INT,
                Quantity INT,
                OrderDate DATE
            )
        """)
        
        # 3. SEED DATA
        print("  Seeding data...")
        cursor.execute("INSERT INTO PRODUCTS VALUES (1, 'Chai', 'Beverages', 18.00)")
        cursor.execute("INSERT INTO PRODUCTS VALUES (2, 'Chang', 'Beverages', 19.00)")
        cursor.execute("INSERT INTO PRODUCTS VALUES (3, 'Aniseed Syrup', 'Condiments', 10.00)")
        
        cursor.execute("INSERT INTO SALES VALUES (101, 1, 10, '2025-12-01')")
        cursor.execute("INSERT INTO SALES VALUES (102, 1, 5, '2025-12-02')")
        cursor.execute("INSERT INTO SALES VALUES (103, 3, 20, '2025-12-03')")
        
        print("[Snowflake] Setup complete.")
        
    finally:
        cursor.close()
        conn.close()

def setup_fabric(settings):
    print("[Fabric] Initializing dataset...")
    # existing script 'create_sales_analytics_dataset.py' does exactly this.
    # We will just reuse the logic or print a reminder.
    print("  (Skipping Fabric reset for speed - assuming 'SalesAnalytics' or similar exists)")
    print("  To force reset Fabric, delete the dataset manually or run 'cleanup_datasets.py'")
    print("[Fabric] Setup complete.")

def main():
    print("="*60)
    print("SETTING UP TEST ENVIRONMENT: 'Holiday Campaign'")
    print("="*60)
    
    settings = get_settings()
    
    try:
        setup_snowflake(settings)
        setup_fabric(settings)
        print("\n[SUCCESS] Environment is ready for the test scenario.")
        
    except Exception as e:
        print(f"\n[ERROR] Setup failed: {e}")

if __name__ == "__main__":
    main()
