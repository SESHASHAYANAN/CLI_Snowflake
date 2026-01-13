-- =============================================================================
-- PERFORMANCE OPTIMIZATION - Snowflake Configuration
-- =============================================================================
-- Optimizations for DirectQuery from Power BI
-- Focus: Clustering, Caching, Materialized Views
-- =============================================================================

USE DATABASE ANALYTICS_DB;

-- -----------------------------------------------------------------------------
-- 1. CLUSTERING KEYS - Optimize micro-partition pruning
-- -----------------------------------------------------------------------------
-- Cluster fact tables by commonly filtered columns

-- Sales fact - cluster by date (most common filter) and customer (common join)
ALTER TABLE CURATED.SALES CLUSTER BY (SALE_DATE, CUSTOMER_ID);

-- If high-cardinality joins are slow, add product clustering
-- ALTER TABLE CURATED.SALES CLUSTER BY (SALE_DATE, PRODUCT_ID);

-- Verify clustering
SELECT 
    TABLE_NAME,
    CLUSTERING_KEY,
    TOTAL_CONSTANT_PARTITION_COUNT,
    AVERAGE_OVERLAPS,
    AVERAGE_DEPTH
FROM TABLE(INFORMATION_SCHEMA.AUTOMATIC_CLUSTERING_HISTORY(
    DATE_RANGE_START => DATEADD(day, -7, CURRENT_DATE()),
    TABLE_NAME => 'CURATED.SALES'
));

-- -----------------------------------------------------------------------------
-- 2. RESULT CACHING - Enable for repeated queries
-- -----------------------------------------------------------------------------
-- Enable at account level (if not already)
ALTER ACCOUNT SET USE_CACHED_RESULT = TRUE;

-- Session-level verification
SHOW PARAMETERS LIKE 'USE_CACHED_RESULT';

-- To check cache usage
SELECT 
    QUERY_ID,
    QUERY_TEXT,
    BYTES_SCANNED,
    BYTES_SCANNED_FROM_RESULT_CACHE,
    PERCENTAGE_SCANNED_FROM_CACHE
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC
LIMIT 20;

-- -----------------------------------------------------------------------------
-- 3. MATERIALIZED VIEWS - Pre-compute expensive aggregations
-- -----------------------------------------------------------------------------
USE SCHEMA SEMANTIC_LAYER;

-- Daily sales summary - frequently accessed, slow to compute
CREATE OR REPLACE MATERIALIZED VIEW MV_DAILY_SALES_SUMMARY
CLUSTER BY (SALE_DATE)
AS
SELECT 
    SALE_DATE,
    SUM(NET_REVENUE) AS DAILY_REVENUE,
    SUM(GROSS_PROFIT) AS DAILY_PROFIT,
    COUNT(*) AS TRANSACTION_COUNT,
    COUNT(DISTINCT CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    SUM(QUANTITY) AS UNITS_SOLD
FROM V_FACT_SALES
GROUP BY SALE_DATE;

-- Monthly by region - common dashboard slice
CREATE OR REPLACE MATERIALIZED VIEW MV_MONTHLY_REGION_SUMMARY
AS
SELECT 
    DATE_TRUNC('MONTH', s.SALE_DATE) AS MONTH,
    c.REGION,
    SUM(s.NET_REVENUE) AS TOTAL_REVENUE,
    SUM(s.GROSS_PROFIT) AS TOTAL_PROFIT,
    COUNT(DISTINCT s.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    COUNT(*) AS TRANSACTIONS
FROM V_FACT_SALES s
JOIN V_DIM_CUSTOMER c ON s.CUSTOMER_ID = c.CUSTOMER_ID
GROUP BY 1, 2;

-- Category performance - product analytics
CREATE OR REPLACE MATERIALIZED VIEW MV_CATEGORY_PERFORMANCE
AS
SELECT 
    p.CATEGORY,
    p.SUBCATEGORY,
    DATE_TRUNC('MONTH', s.SALE_DATE) AS MONTH,
    SUM(s.NET_REVENUE) AS TOTAL_REVENUE,
    SUM(s.GROSS_PROFIT) AS TOTAL_PROFIT,
    SUM(s.QUANTITY) AS UNITS_SOLD,
    COUNT(DISTINCT s.CUSTOMER_ID) AS UNIQUE_BUYERS
FROM V_FACT_SALES s
JOIN V_DIM_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
GROUP BY 1, 2, 3;

-- -----------------------------------------------------------------------------
-- 4. WAREHOUSE CONFIGURATION - Cost optimization
-- -----------------------------------------------------------------------------
-- Create optimized warehouse for Power BI queries
CREATE WAREHOUSE IF NOT EXISTS POWERBI_WH
WITH 
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60          -- Suspend after 1 minute idle
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    SCALING_POLICY = 'ECONOMY' -- Cost-optimized scaling
    COMMENT = 'Dedicated warehouse for Power BI DirectQuery workloads';

-- For heavy dashboards, use multi-cluster
-- ALTER WAREHOUSE POWERBI_WH SET 
--     MIN_CLUSTER_COUNT = 1
--     MAX_CLUSTER_COUNT = 3
--     SCALING_POLICY = 'STANDARD';

-- -----------------------------------------------------------------------------
-- 5. SEARCH OPTIMIZATION - Faster point lookups
-- -----------------------------------------------------------------------------
-- Enable search optimization for dimension lookups
ALTER TABLE CURATED.CUSTOMERS ADD SEARCH OPTIMIZATION ON EQUALITY(CUSTOMER_ID);
ALTER TABLE CURATED.PRODUCTS ADD SEARCH OPTIMIZATION ON EQUALITY(PRODUCT_ID);

-- For text searches
ALTER TABLE CURATED.CUSTOMERS ADD SEARCH OPTIMIZATION ON SUBSTRING(CUSTOMER_NAME);

-- Check search optimization status
SHOW TABLES LIKE '%CUSTOMERS%' IN SCHEMA CURATED;

-- -----------------------------------------------------------------------------
-- 6. QUERY ACCELERATION - Speed up ad-hoc queries
-- -----------------------------------------------------------------------------
-- Enable for the Power BI warehouse
ALTER WAREHOUSE POWERBI_WH SET 
    ENABLE_QUERY_ACCELERATION = TRUE
    QUERY_ACCELERATION_MAX_SCALE_FACTOR = 8;

-- -----------------------------------------------------------------------------
-- 7. STATISTICS COLLECTION - Better query plans
-- -----------------------------------------------------------------------------
-- Analyze tables for optimal execution plans
-- Snowflake handles this automatically, but can be forced
ALTER TABLE CURATED.SALES SET DATA_RETENTION_TIME_IN_DAYS = 7;

-- -----------------------------------------------------------------------------
-- MONITORING QUERIES
-- -----------------------------------------------------------------------------
-- Check warehouse utilization
SELECT 
    WAREHOUSE_NAME,
    AVG(AVG_RUNNING) AS AVG_QUERIES_RUNNING,
    AVG(AVG_QUEUED_LOAD) AS AVG_QUEUE_SIZE,
    SUM(CREDITS_USED) AS TOTAL_CREDITS
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_LOAD_HISTORY
WHERE START_TIME >= DATEADD(day, -7, CURRENT_DATE())
GROUP BY WAREHOUSE_NAME
ORDER BY TOTAL_CREDITS DESC;

-- Slow query identification
SELECT 
    QUERY_ID,
    USER_NAME,
    WAREHOUSE_NAME,
    TOTAL_ELAPSED_TIME / 1000 AS ELAPSED_SECONDS,
    BYTES_SCANNED / 1024 / 1024 AS MB_SCANNED,
    ROWS_PRODUCED,
    QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE START_TIME >= DATEADD(day, -1, CURRENT_DATE())
AND TOTAL_ELAPSED_TIME > 10000  -- Queries over 10 seconds
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;
