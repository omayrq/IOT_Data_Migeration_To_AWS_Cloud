-- =============================================================================
-- setup_db.sql — IoT Streaming Platform PostgreSQL Schema
-- Run as: psql -h <RDS_ENDPOINT> -U postgres -d iot_streaming_db -f setup_db.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Create dedicated application user (do NOT use the master postgres user)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'iot_app') THEN
        CREATE ROLE iot_app WITH LOGIN PASSWORD 'REPLACE_WITH_SECRETS_MANAGER_VALUE';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'debezium') THEN
        -- Debezium needs REPLICATION + LOGIN + access to tables
        CREATE ROLE debezium WITH LOGIN PASSWORD 'REPLACE_WITH_SECRETS_MANAGER_VALUE' REPLICATION;
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Schema
-- ---------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS iot AUTHORIZATION iot_app;
SET search_path = iot, public;

-- Grant Debezium read access to schema
GRANT USAGE ON SCHEMA iot TO debezium;

-- ---------------------------------------------------------------------------
-- device_registry: Master device catalog
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.device_registry (
    device_id        VARCHAR(50)  PRIMARY KEY,
    region           VARCHAR(50),
    model            VARCHAR(100),
    firmware_version VARCHAR(20),
    registered_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_seen_at     TIMESTAMPTZ,
    is_active        BOOLEAN      NOT NULL DEFAULT TRUE,
    metadata         JSONB        DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_device_registry_region ON iot.device_registry (region);
CREATE INDEX IF NOT EXISTS idx_device_registry_active ON iot.device_registry (is_active) WHERE is_active = TRUE;

-- ---------------------------------------------------------------------------
-- iot_events: Core telemetry table (append-only time-series)
-- Optimised for Debezium CDC: single-column PK with auto-id
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.iot_events (
    id               BIGSERIAL    PRIMARY KEY,
    device_id        VARCHAR(50)  NOT NULL,
    region           VARCHAR(50),
    latitude         DOUBLE PRECISION NOT NULL,
    longitude        DOUBLE PRECISION NOT NULL,
    event_timestamp  TIMESTAMPTZ  NOT NULL,
    ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    temperature      DOUBLE PRECISION NOT NULL,
    humidity         DOUBLE PRECISION NOT NULL,
    battery          DOUBLE PRECISION NOT NULL,
    speed            DOUBLE PRECISION NOT NULL DEFAULT 0,
    signal_strength  INTEGER      NOT NULL,
    is_online        BOOLEAN      NOT NULL DEFAULT TRUE,
    event_type       VARCHAR(50)  NOT NULL DEFAULT 'telemetry',
    raw_payload      JSONB,
    CONSTRAINT fk_device FOREIGN KEY (device_id)
        REFERENCES iot.device_registry (device_id) ON DELETE RESTRICT
);

-- Time-series queries: latest events per device
CREATE INDEX IF NOT EXISTS idx_iot_events_device_time
    ON iot.iot_events (device_id, event_timestamp DESC);

-- Range queries: all events in a time window
CREATE INDEX IF NOT EXISTS idx_iot_events_timestamp
    ON iot.iot_events (event_timestamp DESC);

-- Region-based analytics
CREATE INDEX IF NOT EXISTS idx_iot_events_region
    ON iot.iot_events (region, event_timestamp DESC);

-- Alert queries: low battery / offline devices
CREATE INDEX IF NOT EXISTS idx_iot_events_battery
    ON iot.iot_events (battery) WHERE battery < 20.0;

CREATE INDEX IF NOT EXISTS idx_iot_events_offline
    ON iot.iot_events (device_id) WHERE is_online = FALSE;

-- ---------------------------------------------------------------------------
-- event_processing_log: Dedup / replay guard
-- Tracks which Kafka offsets have been processed
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.event_processing_log (
    kafka_topic      VARCHAR(100) NOT NULL,
    kafka_partition  INTEGER      NOT NULL,
    kafka_offset     BIGINT       NOT NULL,
    processed_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    event_count      INTEGER      NOT NULL DEFAULT 1,
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
);

-- ---------------------------------------------------------------------------
-- device_alerts: Generated alerts for anomalies
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.device_alerts (
    id               BIGSERIAL    PRIMARY KEY,
    device_id        VARCHAR(50)  NOT NULL,
    alert_type       VARCHAR(50)  NOT NULL,  -- 'low_battery','high_temp','offline'
    severity         VARCHAR(20)  NOT NULL DEFAULT 'warning', -- 'info','warning','critical'
    message          TEXT,
    triggered_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    resolved_at      TIMESTAMPTZ,
    is_resolved      BOOLEAN      NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_alerts_device ON iot.device_alerts (device_id, triggered_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_unresolved ON iot.device_alerts (is_resolved) WHERE is_resolved = FALSE;

-- ---------------------------------------------------------------------------
-- Permissions
-- ---------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot TO iot_app;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA iot TO iot_app;
GRANT SELECT ON ALL TABLES IN SCHEMA iot TO debezium;
ALTER DEFAULT PRIVILEGES IN SCHEMA iot GRANT SELECT ON TABLES TO debezium;

-- ---------------------------------------------------------------------------
-- Logical Replication Publication (for Debezium CDC)
-- ---------------------------------------------------------------------------
-- Create publication ONLY if it doesn't exist (RDS may not support IF NOT EXISTS)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_publication WHERE pubname = 'iot_publication') THEN
        EXECUTE 'CREATE PUBLICATION iot_publication FOR TABLE
            iot.iot_events,
            iot.device_registry,
            iot.device_alerts';
    END IF;
END
$$;

-- ---------------------------------------------------------------------------
-- Seed: Insert 100 device records (matches simulator)
-- ---------------------------------------------------------------------------
INSERT INTO iot.device_registry (device_id, region, model, firmware_version)
SELECT
    'device_' || LPAD(generate_series::text, 4, '0'),
    (ARRAY['new_york','los_angeles','chicago','houston','miami'])[1 + (generate_series % 5)],
    (ARRAY['TempSensor-Pro','HumidBot-v2','FleetTracker-X'])[1 + (generate_series % 3)],
    'v' || (1 + generate_series % 3) || '.' || (generate_series % 10) || '.0'
FROM generate_series(1, 100)
ON CONFLICT (device_id) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Verify
-- ---------------------------------------------------------------------------
SELECT 'device_registry row count: ' || COUNT(*) AS check FROM iot.device_registry;
SELECT 'iot_publication exists: ' || COUNT(*) AS check FROM pg_publication WHERE pubname = 'iot_publication';
SELECT 'logical_replication param: ' || setting AS check FROM pg_settings WHERE name = 'rds.logical_replication';
