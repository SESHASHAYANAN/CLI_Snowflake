-- ============================================
-- Snowflake Verification Queries
-- Fabric Samples Metadata Sync Results
-- ============================================

USE DATABASE ANALYTICS_DB;
USE SCHEMA SEMANTIC_LAYER;

-- ============================================
-- 1. CHECK SEMANTIC METADATA
-- ============================================
SELECT 
    MODEL_ID,
    MODEL_NAME,
    SOURCE_SYSTEM,
    TABLE_COUNT,
    SYNC_VERSION,
    CREATED_AT,
    UPDATED_AT
FROM _SEMANTIC_METADATA
ORDER BY UPDATED_AT DESC;

-- ============================================
-- 2. VIEW ALL MEASURES
-- ============================================
SELECT 
    MEASURE_NAME,
    TABLE_NAME,
    EXPRESSION,
    DESCRIPTION,
    FORMAT_STRING
FROM _SEMANTIC_MEASURES
ORDER BY TABLE_NAME, MEASURE_NAME;

-- ============================================
-- 3. CHECK RELATIONSHIPS
-- ============================================
SELECT 
    RELATIONSHIP_ID,
    FROM_TABLE,
    FROM_COLUMN,
    TO_TABLE,
    TO_COLUMN,
    CARDINALITY,
    IS_ACTIVE
FROM _SEMANTIC_RELATIONSHIPS
ORDER BY FROM_TABLE;

-- ============================================
-- 4. VIEW SYNC HISTORY
-- ============================================
SELECT 
    SYNC_ID,
    RUN_ID,
    STARTED_AT,
    COMPLETED_AT,
    STATUS,
    CHANGES_APPLIED,
    TOTAL_CHANGES,
    ERROR_COUNT,
    DATEDIFF('second', STARTED_AT, COMPLETED_AT) as DURATION_SECONDS
FROM _SEMANTIC_SYNC_HISTORY
ORDER BY STARTED_AT DESC
LIMIT 10;

-- ============================================
-- 5. LIST ALL TABLES IN SEMANTIC_LAYER
-- ============================================
SHOW TABLES IN ANALYTICS_DB.SEMANTIC_LAYER;

-- ============================================
-- 6. LIST ALL VIEWS (Measures)
-- ============================================
SHOW VIEWS IN ANALYTICS_DB.SEMANTIC_LAYER;

-- ============================================
-- 7. GET TABLE DETAILS WITH COMMENTS
-- ============================================
SELECT 
    TABLE_SCHEMA,
    TABLE_NAME,
    COMMENT as TABLE_DESCRIPTION,
    ROW_COUNT,
    BYTES,
    CREATED as CREATED_AT
FROM ANALYTICS_DB.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;

-- ============================================
-- 8. GET COLUMN METADATA
-- ============================================
SELECT 
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    IS_NULLABLE,
    COMMENT as COLUMN_DESCRIPTION
FROM ANALYTICS_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'SEMANTIC_LAYER'
  AND TABLE_NAME NOT LIKE '_SEMANTIC%'
ORDER BY TABLE_NAME, ORDINAL_POSITION;

-- ============================================
-- 9. VERIFY MODEL JSON STRUCTURE
-- ============================================
SELECT 
    MODEL_NAME,
    MODEL_JSON:name::STRING as json_model_name,
    MODEL_JSON:source::STRING as source_system,
    MODEL_JSON:description::STRING as description,
    ARRAY_SIZE(MODEL_JSON:tables) as tables_count,
    ARRAY_SIZE(MODEL_JSON:measures) as measures_count,
    ARRAY_SIZE(MODEL_JSON:relationships) as relationships_count
FROM _SEMANTIC_METADATA
ORDER BY UPDATED_AT DESC;

-- ============================================
-- 10. SAMPLE: Test a Measure View (if exists)
-- ============================================
-- Uncomment to test specific views:
-- SELECT * FROM VW_TOTAL_REVENUE LIMIT 10;
-- SELECT * FROM VW_TOTAL_ORDERS LIMIT 10;
