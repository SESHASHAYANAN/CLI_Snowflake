-- =============================================================================
-- POWER BI DIRECTQUERY SETUP - Snowflake Configuration
-- =============================================================================
-- Prepare Snowflake for optimal Power BI DirectQuery connections
-- =============================================================================

USE DATABASE ANALYTICS_DB;

-- -----------------------------------------------------------------------------
-- 1. CREATE DEDICATED POWERBI ROLE
-- -----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

CREATE ROLE IF NOT EXISTS POWERBI_SERVICE;
COMMENT ON ROLE POWERBI_SERVICE IS 'Service role for Power BI DirectQuery connections';

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE POWERBI_WH TO ROLE POWERBI_SERVICE;
GRANT OPERATE ON WAREHOUSE POWERBI_WH TO ROLE POWERBI_SERVICE;

-- Grant database/schema access
GRANT USAGE ON DATABASE ANALYTICS_DB TO ROLE POWERBI_SERVICE;
GRANT USAGE ON SCHEMA ANALYTICS_DB.SEMANTIC_LAYER TO ROLE POWERBI_SERVICE;

-- Grant read access to semantic views
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS_DB.SEMANTIC_LAYER TO ROLE POWERBI_SERVICE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS_DB.SEMANTIC_LAYER TO ROLE POWERBI_SERVICE;

-- Grant access to materialized views
GRANT SELECT ON ALL MATERIALIZED VIEWS IN SCHEMA ANALYTICS_DB.SEMANTIC_LAYER TO ROLE POWERBI_SERVICE;

-- -----------------------------------------------------------------------------
-- 2. CREATE POWERBI SERVICE USER
-- -----------------------------------------------------------------------------
CREATE USER IF NOT EXISTS POWERBI_SVC_USER
    PASSWORD = 'ChangeThis$ecureP@ssw0rd!'
    DEFAULT_ROLE = POWERBI_SERVICE
    DEFAULT_WAREHOUSE = POWERBI_WH
    DEFAULT_NAMESPACE = ANALYTICS_DB.SEMANTIC_LAYER
    MUST_CHANGE_PASSWORD = FALSE
    COMMENT = 'Service account for Power BI DirectQuery';

GRANT ROLE POWERBI_SERVICE TO USER POWERBI_SVC_USER;

-- -----------------------------------------------------------------------------
-- 3. NETWORK POLICY (Optional - for IP whitelisting)
-- -----------------------------------------------------------------------------
-- Restrict connections to known Power BI service IPs
-- CREATE NETWORK POLICY IF NOT EXISTS POWERBI_NETWORK_POLICY
--     ALLOWED_IP_LIST = (
--         '20.21.32.0/22',      -- Power BI Service IPs (example)
--         '40.74.0.0/15'        -- Azure IPs
--     )
--     COMMENT = 'Restrict access to Power BI service IPs';
-- 
-- ALTER USER POWERBI_SVC_USER SET NETWORK_POLICY = POWERBI_NETWORK_POLICY;

-- -----------------------------------------------------------------------------
-- 4. QUERY TAG FOR MONITORING
-- -----------------------------------------------------------------------------
-- Set query tag for Power BI queries (helps with cost allocation)
ALTER USER POWERBI_SVC_USER SET QUERY_TAG = 'PowerBI_DirectQuery';

-- Monitor Power BI queries
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY 
-- WHERE QUERY_TAG = 'PowerBI_DirectQuery'
-- AND START_TIME >= DATEADD(day, -1, CURRENT_DATE());

-- -----------------------------------------------------------------------------
-- 5. SESSION PARAMETERS FOR DIRECTQUERY OPTIMIZATION
-- -----------------------------------------------------------------------------
-- These can be set at user or session level
ALTER USER POWERBI_SVC_USER SET
    STATEMENT_TIMEOUT_IN_SECONDS = 300,      -- 5 min timeout
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = 60, -- 1 min queue timeout
    CLIENT_SESSION_KEEP_ALIVE = TRUE,
    AUTOCOMMIT = TRUE;

-- -----------------------------------------------------------------------------
-- 6. POWER BI CONNECTION STRING DETAILS
-- -----------------------------------------------------------------------------
/*
Power BI Desktop Connection Settings:
=====================================
Server:     FA97567.central-india.azure.snowflakecomputing.com
Warehouse:  POWERBI_WH
Database:   ANALYTICS_DB
Schema:     SEMANTIC_LAYER (optional, can set in queries)

Authentication:
- Username: POWERBI_SVC_USER
- Password: <configured above>
- OR use Azure AD SSO if configured

Connection Mode:
- DirectQuery (recommended for real-time)
- Import (for large historical datasets)

Advanced Options:
- Role: POWERBI_SERVICE
- Keep Connection Open: Yes

Power BI Service (Gateway-less):
================================
Snowflake connector supports direct cloud-to-cloud connectivity.
No on-premises gateway required.

Semantic Model Configuration:
=============================
1. Connect to Views:
   - V_DIM_CUSTOMER
   - V_DIM_PRODUCT
   - V_DIM_DATE
   - V_FACT_SALES
   - V_AGG_SALES_SUMMARY (for dashboards)

2. Relationships (auto-detected from view joins):
   - V_FACT_SALES.CUSTOMER_ID → V_DIM_CUSTOMER.CUSTOMER_ID
   - V_FACT_SALES.PRODUCT_ID → V_DIM_PRODUCT.PRODUCT_ID
   - V_FACT_SALES.SALE_DATE → V_DIM_DATE.FULL_DATE

3. Measures (minimal - most pre-calculated in Snowflake):
   - Total Revenue = SUM(V_FACT_SALES[NET_REVENUE])
   - Total Profit = SUM(V_FACT_SALES[GROSS_PROFIT])
   - YoY Growth = <DAX calculation>

4. RLS (inherited from Snowflake):
   - Do NOT configure RLS in Power BI
   - Security enforced at Snowflake via row access policies
   - User context passed through DirectQuery

*/

-- -----------------------------------------------------------------------------
-- 7. VERIFICATION QUERIES
-- -----------------------------------------------------------------------------
-- Verify role grants
SHOW GRANTS TO ROLE POWERBI_SERVICE;

-- Verify user configuration
DESCRIBE USER POWERBI_SVC_USER;

-- Test connection as Power BI user
USE ROLE POWERBI_SERVICE;
USE WAREHOUSE POWERBI_WH;
USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- Run sample queries to verify access
SELECT COUNT(*) AS customer_count FROM V_DIM_CUSTOMER;
SELECT COUNT(*) AS product_count FROM V_DIM_PRODUCT;
SELECT COUNT(*) AS sales_count FROM V_FACT_SALES;

-- Check query performance
SELECT 
    TABLE_NAME,
    ROW_COUNT,
    BYTES,
    BYTES / 1024 / 1024 AS MB
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
ORDER BY BYTES DESC;
