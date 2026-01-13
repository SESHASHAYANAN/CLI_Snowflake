# 🔍 VIEW SNOWFLAKE DATABASE - COMPLETE GUIDE

## Method 1: Snowflake Web UI (Account Login)

### Login to Snowflake
**URL:** https://fa97567.central-india.azure.snowflakecomputing.com

**Credentials:**
- Account: FA97567.central-india.azure
- User: SYNC_SERVICE
- Password: (from your .env file)

### SQL Commands to Run in Snowflake Worksheet

```sql
-- ============================================
-- 1. SET CONTEXT
-- ============================================
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;
USE WAREHOUSE COMPUTE_WAREHOUSE;

-- ============================================
-- 2. VIEW ALL TABLES
-- ============================================
SHOW TABLES IN ANALYTICS_DB.SEMANTIC_LAYER;

-- ============================================
-- 3. VIEW ALL SYNCED MODELS
-- ============================================
SELECT 
    MODEL_NAME,
    SOURCE_SYSTEM,
    TABLE_COUNT,
    SYNC_VERSION,
    CREATED_AT,
    UPDATED_AT
FROM _SEMANTIC_METADATA
ORDER BY UPDATED_AT DESC;

-- ============================================
-- 4. VIEW ALL DATA TABLES
-- ============================================
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES / 1024 as SIZE_KB,
    CREATED as CREATED_AT
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- ============================================
-- 5. QUERY DEMO_PRODUCTS TABLE
-- ============================================
SELECT * FROM DEMO_PRODUCTS;

-- ============================================
-- 6. QUERY SALES_DATA TABLE
-- ============================================
SELECT * FROM SALES_DATA ORDER BY ORDER_DATE DESC;

-- ============================================
-- 7. QUERY MY_NEW_TABLE
-- ============================================
SELECT * FROM MY_NEW_TABLE;

-- ============================================
-- 8. VIEW SYNC HISTORY
-- ============================================
SELECT 
    SYNC_ID,
    RUN_ID,
    STARTED_AT,
    STATUS,
    CHANGES_APPLIED
FROM _SEMANTIC_SYNC_HISTORY
ORDER BY STARTED_AT DESC
LIMIT 20;

-- ============================================
-- 9. VIEW ALL VIEWS
-- ============================================
SHOW VIEWS IN ANALYTICS_DB.SEMANTIC_LAYER;

-- ============================================
-- 10. GET TABLE DETAILS WITH COLUMNS
-- ============================================
DESCRIBE TABLE DEMO_PRODUCTS;
DESCRIBE TABLE SALES_DATA;
DESCRIBE TABLE MY_NEW_TABLE;

-- ============================================
-- 11. COUNT ROWS IN ALL TABLES
-- ============================================
SELECT COUNT(*) as total_products FROM DEMO_PRODUCTS;
SELECT COUNT(*) as total_sales FROM SALES_DATA;
SELECT COUNT(*) as total_records FROM MY_NEW_TABLE;

-- ============================================
-- 12. ANALYTICS QUERY EXAMPLE
-- ============================================
SELECT 
    CUSTOMER_NAME,
    PRODUCT_NAME,
    QUANTITY * PRICE as TOTAL_AMOUNT,
    ORDER_DATE
FROM SALES_DATA
ORDER BY TOTAL_AMOUNT DESC;

-- ============================================
-- 13. VIEW COMPLETE MODEL JSON
-- ============================================
SELECT 
    MODEL_NAME,
    MODEL_JSON
FROM _SEMANTIC_METADATA
WHERE MODEL_NAME = 'SnowflakeSync_20260111_211553';
```

---

## Method 2: Terminal Commands (Windows CMD)

### Quick Check Commands

```bash
# Navigate to project directory
cd c:\Users\M.S.Seshashayanan\.gemini\antigravity\scratch\CLI_Snowflake

# View all tables and models (Simplest)
python check_snowflake_tables.py

# View complete data with samples (Comprehensive)
python query_all_snowflake_results.py
```

### Python Script - Quick Query

Create a file `quick_query.py` or run directly:

```python
import sys
import os
sys.path.append(os.getcwd())
from semantic_sync.config import get_settings
import snowflake.connector

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

# Query all products
print("\n=== DEMO_PRODUCTS ===")
cursor.execute("SELECT * FROM DEMO_PRODUCTS")
for row in cursor.fetchall():
    print(row)

# Query all sales
print("\n=== SALES_DATA ===")
cursor.execute("SELECT * FROM SALES_DATA")
for row in cursor.fetchall():
    print(row)

# Query models
print("\n=== SYNCED MODELS ===")
cursor.execute("SELECT MODEL_NAME, TABLE_COUNT FROM _SEMANTIC_METADATA")
for row in cursor.fetchall():
    print(f"Model: {row[0]}, Tables: {row[1]}")

conn.close()
```

### SnowSQL Command Line Tool (Optional)

If you have SnowSQL installed:

```bash
# Connect to Snowflake
snowsql -a FA97567.central-india.azure -u SYNC_SERVICE -d ANALYTICS_DB -s SEMANTIC_LAYER

# Then run SQL commands:
SELECT * FROM DEMO_PRODUCTS;
SELECT * FROM SALES_DATA;
SELECT * FROM _SEMANTIC_METADATA;
```

---

## Method 3: Python Interactive Script

### Create `view_database.py`:

```python
#!/usr/bin/env python3
"""Quick Snowflake Database Viewer"""

import sys
import os
sys.path.append(os.getcwd())
from semantic_sync.config import get_settings
import snowflake.connector

def view_database():
    settings = get_settings()
    conf = settings.get_snowflake_config()
    
    print(f"\nConnecting to {conf.account}...")
    conn = snowflake.connector.connect(
        account=conf.account,
        user=conf.user,
        password=conf.password.get_secret_value(),
        warehouse=conf.warehouse,
        database=conf.database,
        schema=conf.schema_name
    )
    
    cursor = conn.cursor()
    
    # 1. List all tables
    print("\n" + "="*70)
    print("ALL TABLES")
    print("="*70)
    cursor.execute("SHOW TABLES")
    for row in cursor.fetchall():
        print(f"  {row[1]}")  # Table name
    
    # 2. View products
    print("\n" + "="*70)
    print("DEMO_PRODUCTS DATA")
    print("="*70)
    cursor.execute("SELECT * FROM DEMO_PRODUCTS")
    print(f"{'ID':<5} {'Name':<20} {'Category':<15} {'Price':<10}")
    print("-"*70)
    for row in cursor.fetchall():
        print(f"{row[0]:<5} {row[1]:<20} {row[2]:<15} ${row[3]:<10}")
    
    # 3. View sales
    print("\n" + "="*70)
    print("SALES_DATA")
    print("="*70)
    cursor.execute("SELECT * FROM SALES_DATA")
    print(f"{'Order':<8} {'Customer':<20} {'Product':<15} {'Qty':<5} {'Price':<12}")
    print("-"*70)
    for row in cursor.fetchall():
        print(f"{row[0]:<8} {row[1]:<20} {row[2]:<15} {row[3]:<5} ${row[4]:<12}")
    
    # 4. View models
    print("\n" + "="*70)
    print("SYNCED MODELS")
    print("="*70)
    cursor.execute("SELECT MODEL_NAME, TABLE_COUNT FROM _SEMANTIC_METADATA ORDER BY UPDATED_AT DESC")
    for row in cursor.fetchall():
        print(f"  {row[0]:<40} Tables: {row[1]}")
    
    conn.close()
    print("\n" + "="*70)
    print("DATABASE VIEW COMPLETE")
    print("="*70 + "\n")

if __name__ == "__main__":
    view_database()
```

### Run it:
```bash
python view_database.py
```

---

## Method 4: One-Line Terminal Commands

### View specific data:

```bash
# Products only
python -c "import sys,os;sys.path.append(os.getcwd());from semantic_sync.config import get_settings;import snowflake.connector;conf=get_settings().get_snowflake_config();conn=snowflake.connector.connect(account=conf.account,user=conf.user,password=conf.password.get_secret_value(),warehouse=conf.warehouse,database=conf.database,schema=conf.schema_name);cursor=conn.cursor();cursor.execute('SELECT * FROM DEMO_PRODUCTS');[print(row) for row in cursor.fetchall()];conn.close()"

# Sales only
python -c "import sys,os;sys.path.append(os.getcwd());from semantic_sync.config import get_settings;import snowflake.connector;conf=get_settings().get_snowflake_config();conn=snowflake.connector.connect(account=conf.account,user=conf.user,password=conf.password.get_secret_value(),warehouse=conf.warehouse,database=conf.database,schema=conf.schema_name);cursor=conn.cursor();cursor.execute('SELECT * FROM SALES_DATA');[print(row) for row in cursor.fetchall()];conn.close()"
```

---

## Quick Reference Card

### Snowflake Web UI
```
URL: https://fa97567.central-india.azure.snowflakecomputing.com
User: SYNC_SERVICE
Then run: SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA;
```

### Terminal (Simplest)
```bash
cd c:\Users\M.S.Seshashayanan\.gemini\antigravity\scratch\CLI_Snowflake
python check_snowflake_tables.py
```

### Terminal (Most Data)
```bash
python query_all_snowflake_results.py
```

---

## Pro Tips

1. **Fastest**: Use `python check_snowflake_tables.py`
2. **Most Detailed**: Use `python query_all_snowflake_results.py`
3. **Custom Queries**: Log into Snowflake web UI
4. **Automated**: Create the `view_database.py` script above

Choose the method that works best for you!
