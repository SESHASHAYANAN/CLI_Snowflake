# 📍 WHERE IS YOUR DATA SAVED IN SNOWFLAKE?

## 🗂️ EXACT LOCATION

Your synced data is saved in this specific location in Snowflake:

```
SNOWFLAKE ACCOUNT: FA97567.central-india.azure
  └── DATABASE: ANALYTICS_DB
       └── SCHEMA: SEMANTIC_LAYER
            └── TABLES:
                 ├── DEMO_PRODUCTS (3 rows)
                 ├── MY_NEW_TABLE (1 row)
                 ├── SALES_DATA (5 rows)
                 ├── _SEMANTIC_MEASURES
                 ├── _SEMANTIC_METADATA (11 models)
                 ├── _SEMANTIC_RELATIONSHIPS
                 └── _SEMANTIC_SYNC_HISTORY
```

---

## 🎯 HOW TO FIND IT IN SNOWFLAKE

### METHOD 1: DATA BROWSER (Easiest - Click Through)

**Step-by-Step:**

1. **Login to Snowflake:**
   - URL: https://fa97567.central-india.azure.snowflakecomputing.com
   - User: SYNC_SERVICE
   - Password: StrongPassword@123

2. **Click "Data" in the left sidebar**

3. **Click "Databases"**

4. **Find and click "ANALYTICS_DB"**
   - This is where your data is stored!

5. **Click "SEMANTIC_LAYER"**
   - This is the folder/schema inside the database

6. **Click "Tables"**
   - Now you see all 7 tables!

7. **Click any table name (e.g., "DEMO_PRODUCTS")**
   - You'll see:
     - Column names and types
     - Preview of actual data
     - Row count

**Visual Path:**
```
Snowflake Home
  → Data (sidebar)
    → Databases
      → ANALYTICS_DB ← YOUR DATABASE
        → SEMANTIC_LAYER ← YOUR SCHEMA  
          → Tables ← YOUR DATA HERE!
            → DEMO_PRODUCTS
            → SALES_DATA
            → MY_NEW_TABLE
            → etc.
```

---

### METHOD 2: SQL WORKSHEET (Using Commands)

**Step 1:** Click "Worksheets" in Snowflake

**Step 2:** Run these commands to navigate to your data:

```sql
-- Navigate to your data location
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;        -- This is where it's saved
USE SCHEMA SEMANTIC_LAYER;        -- This is the subfolder

-- Now you're in the right location!
-- Show everything that's here:
SHOW TABLES;

-- View the actual data:
SELECT * FROM DEMO_PRODUCTS;
SELECT * FROM SALES_DATA;
```

**What This Means:**
- `ANALYTICS_DB` = The "hard drive" where data is stored
- `SEMANTIC_LAYER` = The "folder" inside that hard drive
- `DEMO_PRODUCTS` = A "file" (table) in that folder

---

## 📊 VERIFY THE LOCATION

Run this to see exactly where you are:

```sql
SELECT 
    CURRENT_DATABASE() as where_is_data,
    CURRENT_SCHEMA() as which_folder,
    CURRENT_WAREHOUSE() as processing_power;
```

**Expected Result:**
```
WHERE_IS_DATA    | WHICH_FOLDER   | PROCESSING_POWER
ANALYTICS_DB     | SEMANTIC_LAYER | COMPUTE_WAREHOUSE
```

This confirms your data is in `ANALYTICS_DB.SEMANTIC_LAYER`

---

## 🔍 CHECK WHAT'S SAVED

### See All Tables in Your Location:

```sql
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES / 1024 as SIZE_KB,
    CREATED as CREATED_DATE
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY TABLE_NAME;
```

**This shows:**
- Table names
- How many rows each has
- Size of each table
- When it was created

---

## 📂 STORAGE HIERARCHY EXPLAINED

Think of Snowflake like a filing cabinet:

```
🏢 SNOWFLAKE ACCOUNT (FA97567.central-india.azure)
    ↓
📁 DATABASE: ANALYTICS_DB
    ↓
📂 SCHEMA: SEMANTIC_LAYER
    ↓
📄 TABLE: DEMO_PRODUCTS (your actual data)
📄 TABLE: SALES_DATA (your actual data)
📄 TABLE: MY_NEW_TABLE (your actual data)
```

**Full Path to Your Data:**
```
ANALYTICS_DB.SEMANTIC_LAYER.DEMO_PRODUCTS
ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA
ANALYTICS_DB.SEMANTIC_LAYER.MY_NEW_TABLE
```

---

## ✅ COMPLETE CHECK SCRIPT

Run this to verify everything is saved correctly:

```sql
-- ================================================
-- VERIFY DATA LOCATION AND CONTENTS
-- ================================================

-- Step 1: Navigate to the location
USE WAREHOUSE COMPUTE_WAREHOUSE;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- Step 2: Confirm current location
SELECT 
    CURRENT_DATABASE() as saved_in_database,
    CURRENT_SCHEMA() as saved_in_schema;

-- Step 3: List all saved tables
SHOW TABLES IN ANALYTICS_DB.SEMANTIC_LAYER;

-- Step 4: Count what's in each table
SELECT 'DEMO_PRODUCTS' as table_name, COUNT(*) as rows_saved FROM DEMO_PRODUCTS
UNION ALL
SELECT 'SALES_DATA', COUNT(*) FROM SALES_DATA  
UNION ALL
SELECT 'MY_NEW_TABLE', COUNT(*) FROM MY_NEW_TABLE
UNION ALL
SELECT '_SEMANTIC_METADATA', COUNT(*) FROM _SEMANTIC_METADATA;

-- Step 5: View actual saved data
SELECT * FROM DEMO_PRODUCTS;
SELECT * FROM SALES_DATA;

-- Step 6: Check when data was saved
SELECT 
    TABLE_NAME,
    CREATED as SAVED_ON,
    LAST_ALTERED as LAST_UPDATED
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY CREATED DESC;
```

---

## 🎯 QUICK ANSWER

**Q: Where is my data saved?**

**A:** 
- **Account:** FA97567.central-india.azure  
- **Database:** ANALYTICS_DB
- **Schema:** SEMANTIC_LAYER
- **Tables:** 7 tables with all your synced data

**Q: How do I see it?**

**A:** Two ways:

**Method 1 (No SQL):**
```
Login → Data → Databases → ANALYTICS_DB → SEMANTIC_LAYER → Tables
```

**Method 2 (SQL):**
```sql
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;
SHOW TABLES;
SELECT * FROM DEMO_PRODUCTS;
```

---

## 📍 DIRECT ACCESS PATHS

To access your data directly, use these full paths:

```sql
-- Full path format: DATABASE.SCHEMA.TABLE

SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER.DEMO_PRODUCTS;
SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER.SALES_DATA;
SELECT * FROM ANALYTICS_DB.SEMANTIC_LAYER.MY_NEW_TABLE;
```

You can run these from anywhere in Snowflake without setting context!

---

## ✅ VERIFICATION CHECKLIST

- [ ] Can see ANALYTICS_DB in database list
- [ ] Can see SEMANTIC_LAYER in schema list  
- [ ] Can see 7 tables in SEMANTIC_LAYER
- [ ] Can run: `SELECT * FROM DEMO_PRODUCTS;`
- [ ] Can see 3 rows of product data
- [ ] Can run: `SELECT * FROM SALES_DATA;`
- [ ] Can see 5 rows of sales data

**If all checked ✅ = Your data is safely saved and accessible!**

---

**Summary:** Your data is saved in `ANALYTICS_DB.SEMANTIC_LAYER` and accessible through the Data Browser or SQL queries! 🎯
