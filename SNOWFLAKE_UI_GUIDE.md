# 🔍 HOW TO SEE TABLES IN SNOWFLAKE ACCOUNT

## ISSUE: Tables Not Visible in Snowflake UI

**REASON:** You need to set the correct Database and Schema context first!

---

## ✅ SOLUTION: Snowflake Web UI Steps

### Step 1: Login to Snowflake
**URL:** https://fa97567.central-india.azure.snowflakecomputing.com

**Credentials:**
- User: `SYNC_SERVICE`
- Password: `StrongPassword@123`

### Step 2: Open a Worksheet
Click on **"Worksheets"** in the left menu (or top menu)

### Step 3: Run These Commands in Order

**Copy and paste this EXACT sequence:**

```sql
-- ============================================
-- STEP 1: SET THE WAREHOUSE
-- ============================================
USE WAREHOUSE COMPUTE_WAREHOUSE;

-- ============================================
-- STEP 2: SET THE DATABASE
-- ============================================
USE DATABASE ANALYTICS_DB;

-- ============================================
-- STEP 3: SET THE SCHEMA
-- ============================================
USE SCHEMA SEMANTIC_LAYER;

-- ============================================
-- STEP 4: NOW VIEW ALL TABLES
-- ============================================
SHOW TABLES;

-- ============================================
-- ALTERNATIVE: List tables with details
-- ============================================
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    ROW_COUNT,
    CREATED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY TABLE_NAME;

-- ============================================
-- STEP 5: QUERY THE DATA
-- ============================================
SELECT * FROM DEMO_PRODUCTS;
SELECT * FROM SALES_DATA;
SELECT * FROM _SEMANTIC_METADATA;
```

---

## 📋 What You Should See After Running SHOW TABLES:

```
DEMO_PRODUCTS
MY_NEW_TABLE
SALES_DATA
_SEMANTIC_MEASURES
_SEMANTIC_METADATA
_SEMANTIC_RELATIONSHIPS
_SEMANTIC_SYNC_HISTORY
```

---

## 🖥️ VERIFY FROM TERMINAL

Run this to confirm tables exist:

```bash
cd c:\Users\M.S.Seshashayanan\.gemini\antigravity\scratch\CLI_Snowflake
python check_snowflake_tables.py
```

---

## 🔍 TROUBLESHOOTING

### If you still don't see tables:

**1. Check Database List:**
```sql
SHOW DATABASES;
```
**Expected:** You should see `ANALYTICS_DB` in the list

**2. Check Schema List:**
```sql
SHOW SCHEMAS IN DATABASE ANALYTICS_DB;
```
**Expected:** You should see `SEMANTIC_LAYER` in the list

**3. Verify Warehouse:**
```sql
SHOW WAREHOUSES;
```
**Expected:** You should see `COMPUTE_WAREHOUSE`

**4. Check Your Current Context:**
```sql
SELECT CURRENT_DATABASE(), CURRENT_SCHEMA(), CURRENT_WAREHOUSE();
```
**Expected Result:**
```
CURRENT_DATABASE | CURRENT_SCHEMA | CURRENT_WAREHOUSE
ANALYTICS_DB     | SEMANTIC_LAYER | COMPUTE_WAREHOUSE
```

---

## 🎯 MOST COMMON FIX

**The issue is usually:** Snowflake opens in a different database/schema by default.

**Solution:** Always run these 3 commands first:
```sql
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;
```

Then run:
```sql
SHOW TABLES;
SELECT * FROM DEMO_PRODUCTS;
```

---

## 📱 NAVIGATION IN SNOWFLAKE UI

### Using the Data Tab (Alternative Method):

1. Click **"Data"** in the left sidebar
2. Click **"Databases"**
3. Find and expand **"ANALYTICS_DB"**
4. Expand **"SEMANTIC_LAYER"**
5. Expand **"Tables"**
6. You should see all 7 tables listed!

---

## ✅ COMPLETE VERIFICATION SCRIPT

Copy this entire script and run it in Snowflake Worksheet:

```sql
-- ================================================
-- COMPLETE SNOWFLAKE TABLE VERIFICATION
-- ================================================

-- Set context
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- Verify context
SELECT 
    CURRENT_DATABASE() as current_db,
    CURRENT_SCHEMA() as current_schema,
    CURRENT_WAREHOUSE() as current_warehouse;

-- List all tables
SHOW TABLES IN SCHEMA SEMANTIC_LAYER;

-- Count tables
SELECT COUNT(*) as total_tables 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER';

-- Show table details
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES / 1024 as SIZE_KB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY TABLE_NAME;

-- Query each table
SELECT 'DEMO_PRODUCTS' as table_name, COUNT(*) as rows FROM DEMO_PRODUCTS
UNION ALL
SELECT 'SALES_DATA', COUNT(*) FROM SALES_DATA
UNION ALL
SELECT 'MY_NEW_TABLE', COUNT(*) FROM MY_NEW_TABLE;

-- View sample data
SELECT * FROM DEMO_PRODUCTS LIMIT 5;
SELECT * FROM SALES_DATA LIMIT 5;

-- Show synced models
SELECT MODEL_NAME, TABLE_COUNT, UPDATED_AT 
FROM _SEMANTIC_METADATA 
ORDER BY UPDATED_AT DESC;
```

**Expected Results:**
- ✅ 7 tables shown
- ✅ DEMO_PRODUCTS: 3 rows
- ✅ SALES_DATA: 5 rows  
- ✅ MY_NEW_TABLE: 1 row
- ✅ 11 synced models

---

## 🚨 IF STILL NOT WORKING

Run this diagnostic in terminal:

```bash
cd c:\Users\M.S.Seshashayanan\.gemini\antigravity\scratch\CLI_Snowflake
python -c "import sys,os;sys.path.append(os.getcwd());from semantic_sync.config import get_settings;import snowflake.connector;conf=get_settings().get_snowflake_config();print(f'Account: {conf.account}');print(f'Database: {conf.database}');print(f'Schema: {conf.schema_name}');print(f'Warehouse: {conf.warehouse}')"
```

This will show your connection settings.

---

## 📧 QUICK CHECKLIST

- [ ] Logged into Snowflake web UI
- [ ] Opened a Worksheet
- [ ] Ran: `USE WAREHOUSE COMPUTE_WAREHOUSE;`
- [ ] Ran: `USE DATABASE ANALYTICS_DB;`
- [ ] Ran: `USE SCHEMA SEMANTIC_LAYER;`
- [ ] Ran: `SHOW TABLES;`
- [ ] Can see 7 tables
- [ ] Ran: `SELECT * FROM DEMO_PRODUCTS;`
- [ ] Can see data (3 products)

**If all checked ✅ - Tables are visible!**
