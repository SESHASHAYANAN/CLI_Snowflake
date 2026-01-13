-- =============================================================================
-- SEMANTIC LAYER VIEWS - Snowflake as System of Record
-- =============================================================================
-- Run this script in Snowflake to create the semantic layer
-- All business logic is defined here, consumed by Power BI via DirectQuery
-- =============================================================================

USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- -----------------------------------------------------------------------------
-- DIMENSION: Customer
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_DIM_CUSTOMER AS
SELECT 
    CUSTOMER_ID,
    CUSTOMER_NAME,
    EMAIL,
    SEGMENT,
    REGION,
    COUNTRY,
    CITY,
    -- Derived business logic
    CASE 
        WHEN LIFETIME_VALUE > 10000 THEN 'Premium'
        WHEN LIFETIME_VALUE > 1000 THEN 'Standard'
        ELSE 'Basic' 
    END AS CUSTOMER_TIER,
    DATEDIFF(DAY, FIRST_PURCHASE_DATE, CURRENT_DATE()) AS CUSTOMER_AGE_DAYS,
    CASE 
        WHEN DATEDIFF(DAY, LAST_PURCHASE_DATE, CURRENT_DATE()) <= 30 THEN 'Active'
        WHEN DATEDIFF(DAY, LAST_PURCHASE_DATE, CURRENT_DATE()) <= 90 THEN 'At Risk'
        ELSE 'Churned'
    END AS CUSTOMER_STATUS,
    CREATED_AT,
    UPDATED_AT
FROM CURATED.CUSTOMERS;

COMMENT ON VIEW V_DIM_CUSTOMER IS 'Customer dimension with derived tiers and status. Source: CURATED.CUSTOMERS';

-- -----------------------------------------------------------------------------
-- DIMENSION: Product
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_DIM_PRODUCT AS
SELECT 
    PRODUCT_ID,
    PRODUCT_NAME,
    PRODUCT_CODE,
    CATEGORY,
    SUBCATEGORY,
    BRAND,
    UNIT_PRICE,
    COST,
    -- Derived metrics
    (UNIT_PRICE - COST) AS UNIT_MARGIN,
    ROUND((UNIT_PRICE - COST) / NULLIF(UNIT_PRICE, 0) * 100, 2) AS MARGIN_PCT,
    CASE 
        WHEN (UNIT_PRICE - COST) / NULLIF(UNIT_PRICE, 0) > 0.4 THEN 'High Margin'
        WHEN (UNIT_PRICE - COST) / NULLIF(UNIT_PRICE, 0) > 0.2 THEN 'Medium Margin'
        ELSE 'Low Margin'
    END AS MARGIN_TIER,
    IS_ACTIVE,
    CREATED_AT
FROM CURATED.PRODUCTS;

COMMENT ON VIEW V_DIM_PRODUCT IS 'Product dimension with margin calculations. Source: CURATED.PRODUCTS';

-- -----------------------------------------------------------------------------
-- DIMENSION: Date (Calendar)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_DIM_DATE AS
SELECT 
    DATE_KEY,
    FULL_DATE,
    DAY_OF_WEEK,
    DAY_NAME,
    DAY_OF_MONTH,
    DAY_OF_YEAR,
    WEEK_OF_YEAR,
    MONTH_NUMBER,
    MONTH_NAME,
    QUARTER,
    YEAR,
    -- Fiscal calendar (assuming July start)
    CASE WHEN MONTH_NUMBER >= 7 THEN YEAR ELSE YEAR - 1 END AS FISCAL_YEAR,
    CASE 
        WHEN MONTH_NUMBER >= 7 THEN MONTH_NUMBER - 6 
        ELSE MONTH_NUMBER + 6 
    END AS FISCAL_MONTH,
    IS_WEEKEND,
    IS_HOLIDAY
FROM CURATED.DATE_DIM;

COMMENT ON VIEW V_DIM_DATE IS 'Date dimension with fiscal calendar. Source: CURATED.DATE_DIM';

-- -----------------------------------------------------------------------------
-- FACT: Sales (Transactional)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_FACT_SALES AS
SELECT 
    s.SALE_ID,
    s.SALE_DATE,
    s.CUSTOMER_ID,
    s.PRODUCT_ID,
    s.STORE_ID,
    s.QUANTITY,
    s.UNIT_PRICE AS SALE_UNIT_PRICE,
    s.DISCOUNT_PCT,
    s.REVENUE,
    s.DISCOUNT_AMOUNT,
    s.NET_REVENUE,
    -- Pre-calculated measures for DirectQuery performance
    s.NET_REVENUE - (p.COST * s.QUANTITY) AS GROSS_PROFIT,
    ROUND((s.NET_REVENUE - (p.COST * s.QUANTITY)) / NULLIF(s.NET_REVENUE, 0) * 100, 2) AS PROFIT_MARGIN_PCT,
    -- Date dimensions for slicing
    YEAR(s.SALE_DATE) AS SALE_YEAR,
    MONTH(s.SALE_DATE) AS SALE_MONTH,
    QUARTER(s.SALE_DATE) AS SALE_QUARTER,
    DAYOFWEEK(s.SALE_DATE) AS SALE_DAY_OF_WEEK,
    -- Flags
    CASE WHEN s.DISCOUNT_PCT > 0 THEN 1 ELSE 0 END AS IS_DISCOUNTED,
    s.CREATED_AT
FROM CURATED.SALES s
LEFT JOIN CURATED.PRODUCTS p ON s.PRODUCT_ID = p.PRODUCT_ID;

COMMENT ON VIEW V_FACT_SALES IS 'Sales fact with pre-calculated profit. Source: CURATED.SALES + CURATED.PRODUCTS';

-- -----------------------------------------------------------------------------
-- AGGREGATE: Sales Summary (for Dashboard Performance)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AGG_SALES_SUMMARY AS
SELECT 
    c.SEGMENT AS CUSTOMER_SEGMENT,
    c.REGION,
    c.COUNTRY,
    p.CATEGORY AS PRODUCT_CATEGORY,
    p.SUBCATEGORY AS PRODUCT_SUBCATEGORY,
    DATE_TRUNC('MONTH', s.SALE_DATE) AS MONTH,
    YEAR(s.SALE_DATE) AS YEAR,
    QUARTER(s.SALE_DATE) AS QUARTER,
    -- Aggregated measures
    SUM(s.NET_REVENUE) AS TOTAL_REVENUE,
    SUM(s.GROSS_PROFIT) AS TOTAL_PROFIT,
    SUM(s.QUANTITY) AS TOTAL_UNITS,
    SUM(s.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNTS,
    COUNT(DISTINCT s.CUSTOMER_ID) AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT s.PRODUCT_ID) AS UNIQUE_PRODUCTS,
    COUNT(*) AS TRANSACTION_COUNT,
    -- Derived KPIs
    ROUND(SUM(s.NET_REVENUE) / NULLIF(COUNT(DISTINCT s.CUSTOMER_ID), 0), 2) AS REVENUE_PER_CUSTOMER,
    ROUND(SUM(s.GROSS_PROFIT) / NULLIF(SUM(s.NET_REVENUE), 0) * 100, 2) AS PROFIT_MARGIN_PCT
FROM V_FACT_SALES s
JOIN V_DIM_CUSTOMER c ON s.CUSTOMER_ID = c.CUSTOMER_ID
JOIN V_DIM_PRODUCT p ON s.PRODUCT_ID = p.PRODUCT_ID
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8;

COMMENT ON VIEW V_AGG_SALES_SUMMARY IS 'Pre-aggregated sales for dashboard performance. Materialization recommended.';

-- -----------------------------------------------------------------------------
-- AGGREGATE: Customer Metrics
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AGG_CUSTOMER_METRICS AS
SELECT 
    c.CUSTOMER_ID,
    c.CUSTOMER_NAME,
    c.SEGMENT,
    c.REGION,
    c.CUSTOMER_TIER,
    c.CUSTOMER_STATUS,
    COUNT(s.SALE_ID) AS TOTAL_ORDERS,
    SUM(s.NET_REVENUE) AS TOTAL_SPEND,
    SUM(s.GROSS_PROFIT) AS TOTAL_PROFIT_GENERATED,
    MIN(s.SALE_DATE) AS FIRST_ORDER_DATE,
    MAX(s.SALE_DATE) AS LAST_ORDER_DATE,
    DATEDIFF(DAY, MAX(s.SALE_DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER,
    ROUND(SUM(s.NET_REVENUE) / NULLIF(COUNT(s.SALE_ID), 0), 2) AS AVG_ORDER_VALUE
FROM V_DIM_CUSTOMER c
LEFT JOIN V_FACT_SALES s ON c.CUSTOMER_ID = s.CUSTOMER_ID
GROUP BY 1, 2, 3, 4, 5, 6;

COMMENT ON VIEW V_AGG_CUSTOMER_METRICS IS 'Customer-level aggregated metrics for RFM analysis.';

-- -----------------------------------------------------------------------------
-- AGGREGATE: Product Performance
-- -----------------------------------------------------------------------------
CREATE OR REPLACE VIEW V_AGG_PRODUCT_PERFORMANCE AS
SELECT 
    p.PRODUCT_ID,
    p.PRODUCT_NAME,
    p.CATEGORY,
    p.SUBCATEGORY,
    p.MARGIN_TIER,
    SUM(s.QUANTITY) AS TOTAL_UNITS_SOLD,
    SUM(s.NET_REVENUE) AS TOTAL_REVENUE,
    SUM(s.GROSS_PROFIT) AS TOTAL_PROFIT,
    COUNT(DISTINCT s.CUSTOMER_ID) AS UNIQUE_BUYERS,
    ROUND(SUM(s.GROSS_PROFIT) / NULLIF(SUM(s.NET_REVENUE), 0) * 100, 2) AS REALIZED_MARGIN_PCT
FROM V_DIM_PRODUCT p
LEFT JOIN V_FACT_SALES s ON p.PRODUCT_ID = s.PRODUCT_ID
GROUP BY 1, 2, 3, 4, 5;

COMMENT ON VIEW V_AGG_PRODUCT_PERFORMANCE IS 'Product-level performance metrics.';

-- =============================================================================
-- GRANT PERMISSIONS
-- =============================================================================
GRANT USAGE ON SCHEMA SEMANTIC_LAYER TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA SEMANTIC_LAYER TO ROLE ANALYST;
GRANT USAGE ON SCHEMA SEMANTIC_LAYER TO ROLE POWERBI_SERVICE;
GRANT SELECT ON ALL VIEWS IN SCHEMA SEMANTIC_LAYER TO ROLE POWERBI_SERVICE;

-- =============================================================================
-- VERIFICATION
-- =============================================================================
-- Run these to verify views were created successfully
SELECT TABLE_NAME, TABLE_TYPE, COMMENT 
FROM INFORMATION_SCHEMA.TABLES 
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER' AND TABLE_TYPE = 'VIEW'
ORDER BY TABLE_NAME;
