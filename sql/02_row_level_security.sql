-- =============================================================================
-- ROW-LEVEL SECURITY (RLS) - Snowflake Implementation
-- =============================================================================
-- All security enforced at Snowflake level - single enforcement point
-- Power BI inherits security via DirectQuery passthrough
-- =============================================================================

USE DATABASE ANALYTICS_DB;

-- -----------------------------------------------------------------------------
-- STEP 1: Create Security Schema
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS SECURITY;

-- -----------------------------------------------------------------------------
-- STEP 2: User-Region Access Mapping Table
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SECURITY.USER_REGION_ACCESS (
    USER_EMAIL VARCHAR(255) NOT NULL,
    ALLOWED_REGION VARCHAR(50) NOT NULL,
    ACCESS_LEVEL VARCHAR(20) DEFAULT 'READ',  -- READ, WRITE, ADMIN
    GRANTED_BY VARCHAR(255),
    GRANTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    EXPIRES_AT TIMESTAMP,
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (USER_EMAIL, ALLOWED_REGION)
);

COMMENT ON TABLE SECURITY.USER_REGION_ACCESS IS 'Maps users to regions they can access. Used by RLS policies.';

-- -----------------------------------------------------------------------------
-- STEP 3: Sample Data - Configure User Access
-- -----------------------------------------------------------------------------
INSERT INTO SECURITY.USER_REGION_ACCESS (USER_EMAIL, ALLOWED_REGION, ACCESS_LEVEL, GRANTED_BY) VALUES
-- Regional analysts - can only see their region
('analyst.apac@company.com', 'APAC', 'READ', 'admin@company.com'),
('analyst.emea@company.com', 'EMEA', 'READ', 'admin@company.com'),
('analyst.americas@company.com', 'Americas', 'READ', 'admin@company.com'),

-- Multi-region access
('senior.analyst@company.com', 'APAC', 'READ', 'admin@company.com'),
('senior.analyst@company.com', 'EMEA', 'READ', 'admin@company.com'),

-- Power BI Service Account - limited access for testing
('powerbi.service@company.com', 'APAC', 'READ', 'admin@company.com'),
('powerbi.service@company.com', 'EMEA', 'READ', 'admin@company.com'),
('powerbi.service@company.com', 'Americas', 'READ', 'admin@company.com')
ON CONFLICT DO NOTHING;

-- -----------------------------------------------------------------------------
-- STEP 4: Row Access Policy - Region Based
-- -----------------------------------------------------------------------------
CREATE OR REPLACE ROW ACCESS POLICY SECURITY.POLICY_REGION_ACCESS
AS (REGION_VALUE VARCHAR) RETURNS BOOLEAN ->
    -- Allow if user has explicit access to this region
    EXISTS (
        SELECT 1 
        FROM SECURITY.USER_REGION_ACCESS
        WHERE USER_EMAIL = CURRENT_USER()
        AND ALLOWED_REGION = REGION_VALUE
        AND IS_ACTIVE = TRUE
        AND (EXPIRES_AT IS NULL OR EXPIRES_AT > CURRENT_TIMESTAMP())
    )
    -- Or if user has ADMIN role (full access)
    OR IS_ROLE_IN_SESSION('ACCOUNTADMIN')
    OR IS_ROLE_IN_SESSION('SYSADMIN')
    OR IS_ROLE_IN_SESSION('DATA_ADMIN');

COMMENT ON ROW ACCESS POLICY SECURITY.POLICY_REGION_ACCESS IS 'Filters rows by user region access. Applied to semantic views.';

-- -----------------------------------------------------------------------------
-- STEP 5: Row Access Policy - Customer Segment Based
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SECURITY.USER_SEGMENT_ACCESS (
    USER_EMAIL VARCHAR(255) NOT NULL,
    ALLOWED_SEGMENT VARCHAR(50) NOT NULL,
    GRANTED_BY VARCHAR(255),
    GRANTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    PRIMARY KEY (USER_EMAIL, ALLOWED_SEGMENT)
);

CREATE OR REPLACE ROW ACCESS POLICY SECURITY.POLICY_SEGMENT_ACCESS
AS (SEGMENT_VALUE VARCHAR) RETURNS BOOLEAN ->
    EXISTS (
        SELECT 1 
        FROM SECURITY.USER_SEGMENT_ACCESS
        WHERE USER_EMAIL = CURRENT_USER()
        AND ALLOWED_SEGMENT = SEGMENT_VALUE
        AND IS_ACTIVE = TRUE
    )
    OR IS_ROLE_IN_SESSION('ACCOUNTADMIN')
    OR IS_ROLE_IN_SESSION('SYSADMIN');

-- -----------------------------------------------------------------------------
-- STEP 6: Apply Policies to Semantic Views
-- -----------------------------------------------------------------------------
USE SCHEMA SEMANTIC_LAYER;

-- Apply region policy to customer dimension
ALTER VIEW V_DIM_CUSTOMER 
ADD ROW ACCESS POLICY SECURITY.POLICY_REGION_ACCESS ON (REGION);

-- Apply region policy to aggregated views
ALTER VIEW V_AGG_SALES_SUMMARY 
ADD ROW ACCESS POLICY SECURITY.POLICY_REGION_ACCESS ON (REGION);

ALTER VIEW V_AGG_CUSTOMER_METRICS 
ADD ROW ACCESS POLICY SECURITY.POLICY_REGION_ACCESS ON (REGION);

-- -----------------------------------------------------------------------------
-- STEP 7: Audit Logging
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE SECURITY.ACCESS_AUDIT_LOG (
    LOG_ID NUMBER AUTOINCREMENT PRIMARY KEY,
    USER_EMAIL VARCHAR(255),
    ACTION VARCHAR(50),
    RESOURCE_TYPE VARCHAR(50),
    RESOURCE_NAME VARCHAR(255),
    QUERY_TEXT TEXT,
    ROW_COUNT NUMBER,
    EXECUTED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    SESSION_ID VARCHAR(255),
    CLIENT_IP VARCHAR(50)
);

-- Create a stored procedure for logging (optional)
CREATE OR REPLACE PROCEDURE SECURITY.LOG_ACCESS(
    p_action VARCHAR,
    p_resource_type VARCHAR,
    p_resource_name VARCHAR,
    p_row_count NUMBER
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO SECURITY.ACCESS_AUDIT_LOG (USER_EMAIL, ACTION, RESOURCE_TYPE, RESOURCE_NAME, ROW_COUNT)
    VALUES (CURRENT_USER(), p_action, p_resource_type, p_resource_name, p_row_count);
    RETURN 'Logged';
END;
$$;

-- -----------------------------------------------------------------------------
-- VERIFICATION QUERIES
-- -----------------------------------------------------------------------------
-- Check applied policies
SELECT * FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    POLICY_NAME => 'SECURITY.POLICY_REGION_ACCESS'
));

-- Test RLS as different user (requires EXECUTE AS privilege)
-- ALTER SESSION SET SIMULATED_DATA_SHARING_CONSUMER = 'analyst.apac@company.com';
-- SELECT REGION, COUNT(*) FROM SEMANTIC_LAYER.V_DIM_CUSTOMER GROUP BY REGION;
-- ALTER SESSION UNSET SIMULATED_DATA_SHARING_CONSUMER;

-- View user access mappings
SELECT * FROM SECURITY.USER_REGION_ACCESS WHERE IS_ACTIVE = TRUE;
