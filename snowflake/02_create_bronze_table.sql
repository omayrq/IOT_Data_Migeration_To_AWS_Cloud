-- =============================================================================
-- Bronze / RAW table
-- The Snowflake Kafka Connector auto-creates this table on first message with
-- exactly this shape (two columns: metadata + variant payload). Documented
-- here so the schema is explicit and so you can create it manually if needed.
-- =============================================================================

USE DATABASE HACKATHON_IOT;
USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW.IOT_EVENTS (
    RECORD_METADATA VARIANT,   -- topic, partition, offset, key, timestamp
    RECORD_CONTENT  VARIANT    -- the unwrapped Debezium row: device_id, latitude, ...
);

-- Quick sanity checks after the connector is running:

-- 1) Row count is increasing
SELECT COUNT(*) AS bronze_row_count FROM RAW.IOT_EVENTS;

-- 2) Peek at the most recent CDC payloads
SELECT
    RECORD_CONTENT:device_id::STRING        AS device_id,
    RECORD_CONTENT:latitude::FLOAT          AS latitude,
    RECORD_CONTENT:longitude::FLOAT         AS longitude,
    RECORD_CONTENT:aqi::FLOAT               AS aqi,
    RECORD_CONTENT:severity::STRING         AS severity,
    RECORD_CONTENT:event_ts::STRING         AS event_ts,
    RECORD_CONTENT:__op::STRING             AS cdc_operation,   -- c=create, u=update, d=delete
    RECORD_METADATA:CreateTime::TIMESTAMP_NTZ AS kafka_ingest_time
FROM RAW.IOT_EVENTS
ORDER BY kafka_ingest_time DESC
LIMIT 20;
