# 📊 HOW TO CHECK FILES/DATA IN SNOWFLAKE

## What's in Snowflake?

In Snowflake, you have **TABLES** (not files). Here's how to check them:

---

## 🌐 METHOD 1: SNOWFLAKE WEB UI (Easiest)

### Login:
- **URL:** https://fa97567.central-india.azure.snowflakecomputing.com
- **User:** SYNC_SERVICE
- **Password:** StrongPassword@123

### Option A: Using Worksheets (SQL Queries)

**Step 1:** Click "Worksheets" in the menu

**Step 2:** Run these commands:

```sql
-- Set the location
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- Check all tables
SHOW TABLES;

-- View table list with details
SELECT TABLE_NAME, ROW_COUNT, BYTES
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY TABLE_NAME;
```

### Option B: Using Data Browser (No SQL Needed)

**Step 1:** Click **"Data"** in left sidebar

**Step 2:** Click **"Databases"**

**Step 3:** Navigate:
```
ANALYTICS_DB 
  → SEMANTIC_LAYER 
    → Tables
```

**Step 4:** You'll see all your tables listed!

**Step 5:** Click any table name to:
- See the schema (columns)
- Preview the data
- See row count

---

## 📋 WHAT TABLES YOU HAVE (7 Total)

### Data Tables (Your Synced Files):
1. **DEMO_PRODUCTS** - Product catalog (3 rows)
2. **MY_NEW_TABLE** - Test data (1 row)
3. **SALES_DATA** - Sales transactions (5 rows)

### Metadata Tables (System):
4. **_SEMANTIC_MEASURES** - DAX measures
5. **_SEMANTIC_METADATA** - Model definitions (11 models)
6. **_SEMANTIC_RELATIONSHIPS** - Table relationships
7. **_SEMANTIC_SYNC_HISTORY** - Sync audit log

---

## 🔍 HOW TO VIEW TABLE CONTENTS

### View All Data in a Table:

```sql
-- Set context first
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- View products
SELECT * FROM DEMO_PRODUCTS;

-- View sales
SELECT * FROM SALES_DATA;

-- View test data
SELECT * FROM MY_NEW_TABLE;
```

### Check How Many Rows:

```sql
SELECT COUNT(*) as total_rows FROM DEMO_PRODUCTS;
SELECT COUNT(*) as total_rows FROM SALES_DATA;
SELECT COUNT(*) as total_rows FROM MY_NEW_TABLE;
```

### See Table Structure:

```sql
DESCRIBE TABLE DEMO_PRODUCTS;
DESCRIBE TABLE SALES_DATA;
```

---

## 📊 CHECK SPECIFIC DATA

### View Products:
```sql
SELECT * FROM DEMO_PRODUCTS;
```
**Returns:**
```
ID | PRODUCT_NAME  | CATEGORY | PRICE
1  | Super Widget  | Widgets  | 19.99
2  | Mega Gadget   | Gadgets  | 49.99
3  | Ultra Thingy  | Widgets  | 29.50
```

### View Sales:
```sql
SELECT * FROM SALES_DATA;
```
**Returns:**
```
ORDER_ID | CUSTOMER_NAME | PRODUCT_NAME | QUANTITY | PRICE     | ORDER_DATE
101      | Ravi Kumar    | Laptop       | 1        | 55000.00  | 2025-01-05
102      | Anitha Devi   | Mouse        | 2        | 1200.00   | 2025-01-06
103      | Suresh Raj    | Keyboard     | 1        | 1800.00   | 2025-01-07
104      | Priya Sharma  | Monitor      | 1        | 9500.00   | 2025-01-07
105      | Arun Kumar    | Printer      | 1        | 14500.00  | 2025-01-08
```

### View Synced Models:
```sql
SELECT MODEL_NAME, TABLE_COUNT, UPDATED_AT 
FROM _SEMANTIC_METADATA 
ORDER BY UPDATED_AT DESC;
```

---

## 💻 METHOD 2: TERMINAL COMMANDS

From your computer terminal:

```bash
cd c:\Users\M.S.Seshashayanan\.gemini\antigravity\scratch\CLI_Snowflake

# Quick table list
python check_snowflake_tables.py

# Complete data view
python view_database.py

# Detailed analysis
python query_all_snowflake_results.py
```

---

## 📁 COMPLETE VERIFICATION SCRIPT

Copy this entire block into Snowflake Worksheet:

```sql
-- ================================================
-- COMPLETE DATA CHECK
-- ================================================

USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- 1. List all tables
SHOW TABLES;

-- 2. Count rows in each table
SELECT 
    'DEMO_PRODUCTS' as TABLE_NAME, 
    COUNT(*) as ROWS 
FROM DEMO_PRODUCTS
UNION ALL
SELECT 'SALES_DATA', COUNT(*) FROM SALES_DATA
UNION ALL
SELECT 'MY_NEW_TABLE', COUNT(*) FROM MY_NEW_TABLE
UNION ALL
SELECT '_SEMANTIC_METADATA', COUNT(*) FROM _SEMANTIC_METADATA;

-- 3. View all products
SELECT * FROM DEMO_PRODUCTS;

-- 4. View all sales
SELECT * FROM SALES_DATA;

-- 5. View synced models
SELECT 
    MODEL_NAME,
    TABLE_COUNT,
    SOURCE_SYSTEM,
    UPDATED_AT
FROM _SEMANTIC_METADATA
ORDER BY UPDATED_AT DESC;

-- 6. Get table sizes
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES / 1024 / 1024 as SIZE_MB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY TABLE_NAME;
```

---

## 🎯 QUICK REFERENCE

### Just Want to See Tables?
```sql
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;
SHOW TABLES;
```

### Just Want to See Data?
```sql
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;
SELECT * FROM DEMO_PRODUCTS;
SELECT * FROM SALES_DATA;
```

### Just Want Row Counts?
```sql
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;
SELECT COUNT(*) as products FROM DEMO_PRODUCTS;
SELECT COUNT(*) as sales FROM SALES_DATA;
```

---

## 🔍 SEARCH FOR SPECIFIC DATA

### Find products by category:
```sql
SELECT * FROM DEMO_PRODUCTS WHERE CATEGORY = 'Widgets';
```

### Find sales by customer:
```sql
SELECT * FROM SALES_DATA WHERE CUSTOMER_NAME = 'Ravi Kumar';
```

### Find high-value sales:
```sql
SELECT * FROM SALES_DATA WHERE PRICE > 10000 ORDER BY PRICE DESC;
```

---

## ✅ EXPECTED RESULTS

After running the commands, you should see:

**Tables:** 7 tables
- DEMO_PRODUCTS
- MY_NEW_TABLE
- SALES_DATA
- _SEMANTIC_MEASURES
- _SEMANTIC_METADATA
- _SEMANTIC_RELATIONSHIPS
- _SEMANTIC_SYNC_HISTORY

**Data Rows:**
- DEMO_PRODUCTS: 3 rows
- SALES_DATA: 5 rows
- MY_NEW_TABLE: 1 row
- _SEMANTIC_METADATA: 11 models

**Sample Data:** Products, Sales, Customer info all visible

---

## 💡 TIP: DATA BROWSER IS EASIEST!

For beginners, the **Data Browser** (Data → Databases → ANALYTICS_DB → SEMANTIC_LAYER → Tables) is the easiest way to browse without writing SQL!

Just click through and you can:
- ✅ See all tables
- ✅ View column names
- ✅ Preview data
- ✅ See row counts

No SQL needed!
