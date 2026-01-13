"""
Modify Snowflake schema to test sync back to Fabric.
Creates 'Products' table if missing and adds 'NewPromoCode'.
Reads .env directly to avoid SecretStr issues.
"""
import snowflake.connector
import os

def load_env():
    config = {}
    try:
        with open('.env', 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'): continue
                if '=' in line:
                    key, val = line.split('=', 1)
                    config[key.strip()] = val.strip()
    except Exception as e:
        print(f"Error reading .env: {e}")
    return config

def main():
    print("Connecting to Snowflake (reading .env directly)...")
    env = load_env()
    
    user = env.get('SNOWFLAKE_USER')
    password = env.get('SNOWFLAKE_PASSWORD')
    account = env.get('SNOWFLAKE_ACCOUNT')
    warehouse = env.get('SNOWFLAKE_WAREHOUSE')
    database = env.get('SNOWFLAKE_DATABASE')
    schema = env.get('SNOWFLAKE_SCHEMA', 'PUBLIC')
    
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )
    
    cursor = conn.cursor()
    
    try:
        # Explicitly set context
        print(f"Setting context: DB={database}, Schema={schema}")
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        
        # Check if table exists
        print("Checking for Products table...")
        cursor.execute("SHOW TABLES LIKE 'Products'")
        if not cursor.fetchone():
            print("Table 'Products' not found. Creating it...")
            # Create table mimicking the Fabric one but with an extra column
            create_sql = """
            CREATE TABLE Products (
                ProductID INT,
                ProductName VARCHAR(100),
                Category VARCHAR(50),
                UnitPrice DECIMAL(10, 2),
                NewPromoCode VARCHAR(50)
            )
            """
            cursor.execute(create_sql)
            print("[OK] Table 'Products' created with 'NewPromoCode'.")
        else:
            # Add column if table exists
            print("Adding column 'NewPromoCode' to table 'Products'...")
            try:
                cursor.execute("ALTER TABLE Products ADD COLUMN NewPromoCode VARCHAR(50)")
                print("[OK] Column added.")
            except Exception as e:
                if "already exists" in str(e):
                    print("[INFO] Column already exists.")
                else:
                    raise e
                
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
