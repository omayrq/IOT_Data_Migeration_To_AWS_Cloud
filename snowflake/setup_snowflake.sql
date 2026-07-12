-- =============================================================================
-- setup_snowflake.sql — IoT Streaming Platform Snowflake Setup
-- Run as ACCOUNTADMIN once to provision all objects
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. DATABASES — Bronze / Silver / Gold medallion architecture
-- ---------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS IOT_RAW     DATA_RETENTION_TIME_IN_DAYS = 1  COMMENT = 'Bronze: raw CDC events from Kafka';
CREATE DATABASE IF NOT EXISTS IOT_SILVER  DATA_RETENTION_TIME_IN_DAYS = 7  COMMENT = 'Silver: cleaned, deduplicated events';
CREATE DATABASE IF NOT EXISTS IOT_GOLD    DATA_RETENTION_TIME_IN_DAYS = 90 COMMENT = 'Gold: aggregated KPIs for dashboards';

-- ---------------------------------------------------------------------------
-- 2. WAREHOUSES — Right-sized for each use case
-- ---------------------------------------------------------------------------
-- Ingestion warehouse: always-on for Kafka connector writes
CREATE WAREHOUSE IF NOT EXISTS IOT_INGEST_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'Kafka connector ingestion — auto-suspends after 60s idle';

-- Transformation warehouse: dbt runs
CREATE WAREHOUSE IF NOT EXISTS IOT_TRANSFORM_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND   = 120
    AUTO_RESUME    = TRUE
    COMMENT        = 'dbt transformation jobs';

-- Analytics warehouse: Streamlit + ad-hoc queries
CREATE WAREHOUSE IF NOT EXISTS IOT_ANALYTICS_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 300
    AUTO_RESUME    = TRUE
    COMMENT        = 'Streamlit dashboard and analyst queries';

-- ---------------------------------------------------------------------------
-- 3. ROLES — Least-privilege
-- ---------------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS IOT_KAFKA_ROLE      COMMENT = 'Kafka connector: write to Bronze only';
CREATE ROLE IF NOT EXISTS IOT_DBT_ROLE        COMMENT = 'dbt: read Bronze, write Silver+Gold';
CREATE ROLE IF NOT EXISTS IOT_ANALYST_ROLE    COMMENT = 'Streamlit/Grafana: read Gold only';
CREATE ROLE IF NOT EXISTS IOT_ADMIN_ROLE      COMMENT = 'Full admin for IoT databases';

-- Role hierarchy
GRANT ROLE IOT_KAFKA_ROLE   TO ROLE IOT_ADMIN_ROLE;
GRANT ROLE IOT_DBT_ROLE     TO ROLE IOT_ADMIN_ROLE;
GRANT ROLE IOT_ANALYST_ROLE TO ROLE IOT_ADMIN_ROLE;
GRANT ROLE IOT_ADMIN_ROLE   TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 4. USERS
-- ---------------------------------------------------------------------------
-- Kafka connector user (key-pair auth preferred over password)
CREATE USER IF NOT EXISTS KAFKA_CONNECTOR
    DEFAULT_ROLE      = IOT_KAFKA_ROLE
    DEFAULT_WAREHOUSE = IOT_INGEST_WH
    COMMENT           = 'Kafka Snowflake Connector service account';

-- dbt user
CREATE USER IF NOT EXISTS DBT_USER
    DEFAULT_ROLE      = IOT_DBT_ROLE
    DEFAULT_WAREHOUSE = IOT_TRANSFORM_WH
    COMMENT           = 'dbt transformation service account';

-- Analyst/Streamlit user
CREATE USER IF NOT EXISTS ANALYST_USER
    DEFAULT_ROLE      = IOT_ANALYST_ROLE
    DEFAULT_WAREHOUSE = IOT_ANALYTICS_WH
    COMMENT           = 'Read-only analytics access';

-- Role assignments
GRANT ROLE IOT_KAFKA_ROLE   TO USER KAFKA_CONNECTOR;
GRANT ROLE IOT_DBT_ROLE     TO USER DBT_USER;
GRANT ROLE IOT_ANALYST_ROLE TO USER ANALYST_USER;

-- ---------------------------------------------------------------------------
-- 5. SCHEMAS
-- ---------------------------------------------------------------------------
USE DATABASE IOT_RAW;
CREATE SCHEMA IF NOT EXISTS RAW   DATA_RETENTION_TIME_IN_DAYS = 1;

USE DATABASE IOT_SILVER;
CREATE SCHEMA IF NOT EXISTS CLEAN DATA_RETENTION_TIME_IN_DAYS = 7;

USE DATABASE IOT_GOLD;
CREATE SCHEMA IF NOT EXISTS KPI   DATA_RETENTION_TIME_IN_DAYS = 90;

-- ---------------------------------------------------------------------------
-- 6. WAREHOUSE GRANTS
-- ---------------------------------------------------------------------------
GRANT USAGE ON WAREHOUSE IOT_INGEST_WH    TO ROLE IOT_KAFKA_ROLE;
GRANT USAGE ON WAREHOUSE IOT_TRANSFORM_WH TO ROLE IOT_DBT_ROLE;
GRANT USAGE ON WAREHOUSE IOT_ANALYTICS_WH TO ROLE IOT_ANALYST_ROLE;
GRANT USAGE ON WAREHOUSE IOT_ANALYTICS_WH TO ROLE IOT_DBT_ROLE;

-- ---------------------------------------------------------------------------
-- 7. DATABASE / SCHEMA GRANTS
-- ---------------------------------------------------------------------------
-- Kafka: write-only to Bronze
GRANT USAGE, CREATE TABLE ON DATABASE IOT_RAW TO ROLE IOT_KAFKA_ROLE;
GRANT USAGE, CREATE TABLE ON SCHEMA IOT_RAW.RAW TO ROLE IOT_KAFKA_ROLE;

-- dbt: read Bronze, create in Silver + Gold
GRANT USAGE ON DATABASE IOT_RAW    TO ROLE IOT_DBT_ROLE;
GRANT USAGE ON SCHEMA IOT_RAW.RAW  TO ROLE IOT_DBT_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA IOT_RAW.RAW TO ROLE IOT_DBT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON DATABASE IOT_SILVER TO ROLE IOT_DBT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA IOT_SILVER.CLEAN TO ROLE IOT_DBT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON DATABASE IOT_GOLD TO ROLE IOT_DBT_ROLE;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA IOT_GOLD.KPI TO ROLE IOT_DBT_ROLE;

-- Analyst: read Gold only
GRANT USAGE ON DATABASE IOT_GOLD   TO ROLE IOT_ANALYST_ROLE;
GRANT USAGE ON SCHEMA IOT_GOLD.KPI TO ROLE IOT_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA IOT_GOLD.KPI TO ROLE IOT_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA IOT_GOLD.KPI TO ROLE IOT_ANALYST_ROLE;
GRANT SELECT ON ALL VIEWS IN SCHEMA IOT_GOLD.KPI TO ROLE IOT_ANALYST_ROLE;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA IOT_GOLD.KPI TO ROLE IOT_ANALYST_ROLE;
-- Also read Silver for Streamlit device detail
GRANT USAGE ON DATABASE IOT_SILVER          TO ROLE IOT_ANALYST_ROLE;
GRANT USAGE ON SCHEMA IOT_SILVER.CLEAN      TO ROLE IOT_ANALYST_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA IOT_SILVER.CLEAN TO ROLE IOT_ANALYST_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA IOT_SILVER.CLEAN TO ROLE IOT_ANALYST_ROLE;

-- ---------------------------------------------------------------------------
-- 8. BRONZE RAW TABLE — Kafka Snowflake connector writes here
-- The connector creates RECORD_CONTENT (VARIANT) and RECORD_METADATA columns
-- ---------------------------------------------------------------------------
USE ROLE IOT_KAFKA_ROLE;
USE DATABASE IOT_RAW;
USE SCHEMA RAW;
USE WAREHOUSE IOT_INGEST_WH;

CREATE TABLE IF NOT EXISTS IOT_RAW.RAW.IOT_EVENTS_RAW (
    RECORD_METADATA   VARIANT   COMMENT 'Kafka metadata: offset, partition, timestamp',
    RECORD_CONTENT    VARIANT   COMMENT 'Full CDC payload from Debezium',
    INGESTED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (TO_DATE(INGESTED_AT))
DATA_RETENTION_TIME_IN_DAYS = 1
COMMENT = 'Bronze: raw IoT events from Kafka Snowflake Connector';

-- ---------------------------------------------------------------------------
-- 9. VERIFY
-- ---------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
SHOW DATABASES LIKE 'IOT_%';
SHOW WAREHOUSES LIKE 'IOT_%';
SHOW ROLES LIKE 'IOT_%';
SELECT 'Snowflake setup complete ✅' AS status;
