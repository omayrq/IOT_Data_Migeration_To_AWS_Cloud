-- =============================================================================
-- Snowflake setup: database, medallion schemas, warehouse, service account
-- Run as ACCOUNTADMIN or SYSADMIN
-- =============================================================================

CREATE DATABASE IF NOT EXISTS HACKATHON_IOT
    COMMENT = 'IoT hackathon medallion warehouse: Bronze(RAW) -> Silver(CLEAN) -> Gold(ANALYTICS)';

USE DATABASE HACKATHON_IOT;

-- Bronze = raw CDC landing zone (loaded by the Snowflake Kafka Connector)
CREATE SCHEMA IF NOT EXISTS RAW        COMMENT = 'Bronze layer - raw CDC events, 1:1 with Postgres';
-- Silver = cleaned / validated / typed
CREATE SCHEMA IF NOT EXISTS CLEAN      COMMENT = 'Silver layer - deduplicated, typed, validated';
-- Gold = business aggregates for BI
CREATE SCHEMA IF NOT EXISTS ANALYTICS  COMMENT = 'Gold layer - daily aggregates for dashboards';

-- Dedicated virtual warehouse, small + auto-suspend to control cost
CREATE WAREHOUSE IF NOT EXISTS HACKATHON_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Warehouse for IoT hackathon pipeline (dbt + Streamlit + Kafka connector)';

-- ---------------------------------------------------------------------------
-- Service role + user for the Snowflake Kafka Connector (key-pair auth)
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS KAFKA_CONNECTOR_ROLE;

GRANT USAGE ON DATABASE HACKATHON_IOT TO ROLE KAFKA_CONNECTOR_ROLE;
GRANT USAGE, CREATE TABLE ON SCHEMA HACKATHON_IOT.RAW TO ROLE KAFKA_CONNECTOR_ROLE;
GRANT USAGE ON WAREHOUSE HACKATHON_WH TO ROLE KAFKA_CONNECTOR_ROLE;

CREATE USER IF NOT EXISTS HACKATHON_SVC_USER
    DEFAULT_ROLE = KAFKA_CONNECTOR_ROLE
    DEFAULT_WAREHOUSE = HACKATHON_WH
    RSA_PUBLIC_KEY = '<PASTE_PUBLIC_KEY_HERE>'
    COMMENT = 'Service account used by the Snowflake Kafka Connector (key-pair auth, no password)';

GRANT ROLE KAFKA_CONNECTOR_ROLE TO USER HACKATHON_SVC_USER;

-- ---------------------------------------------------------------------------
-- Role for dbt + Streamlit (read/transform access across all 3 layers)
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS ANALYTICS_ROLE;

GRANT USAGE ON DATABASE HACKATHON_IOT TO ROLE ANALYTICS_ROLE;
GRANT USAGE ON WAREHOUSE HACKATHON_WH TO ROLE ANALYTICS_ROLE;
GRANT USAGE, SELECT ON ALL TABLES IN SCHEMA HACKATHON_IOT.RAW TO ROLE ANALYTICS_ROLE;
GRANT ALL ON SCHEMA HACKATHON_IOT.CLEAN TO ROLE ANALYTICS_ROLE;
GRANT ALL ON SCHEMA HACKATHON_IOT.ANALYTICS TO ROLE ANALYTICS_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HACKATHON_IOT.CLEAN TO ROLE ANALYTICS_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA HACKATHON_IOT.ANALYTICS TO ROLE ANALYTICS_ROLE;

-- Assign ANALYTICS_ROLE to your personal user / dbt service user, e.g.:
-- GRANT ROLE ANALYTICS_ROLE TO USER DBT_SVC_USER;
