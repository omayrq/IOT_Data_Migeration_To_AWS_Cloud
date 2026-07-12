-- =============================================================================
-- On-prem PostgreSQL schema (EC2 simulation) — Task 1.4
-- =============================================================================

-- NOTE: table lives in the default "public" schema on purpose so that the
-- Debezium topic resolves to cdc.public.iot_events, matching the spec.
CREATE TABLE IF NOT EXISTS public.iot_events (
    event_id        BIGSERIAL PRIMARY KEY,
    device_id       VARCHAR(50)   NOT NULL,
    device_type     VARCHAR(50)   DEFAULT 'geo_sensor',
    latitude        DOUBLE PRECISION NOT NULL,
    longitude       DOUBLE PRECISION NOT NULL,
    aqi             NUMERIC(6,2),          -- air quality index reading
    temperature_c   NUMERIC(5,2),
    battery_pct     NUMERIC(5,2),
    severity        VARCHAR(20),           -- normal / warning / critical
    event_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_iot_events_device_ts
    ON public.iot_events (device_id, event_ts DESC);

-- ---------------------------------------------------------------------------
-- Debezium logical replication setup
-- ---------------------------------------------------------------------------
-- wal_level=logical is already set at server level via docker-compose command.
-- Create a replication user (least privilege) for Debezium to use.
DO $$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'debezium_user') THEN
      CREATE ROLE debezium_user WITH REPLICATION LOGIN PASSWORD 'debezium_pw';
   END IF;
END
$$;

GRANT CONNECT ON DATABASE iot_db TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO debezium_user;

-- REPLICA IDENTITY FULL ensures UPDATE/DELETE events carry full row images,
-- which the Silver dbt model relies on to detect the previous state.
ALTER TABLE public.iot_events REPLICA IDENTITY FULL;

-- Publication that Debezium's pgoutput plugin will consume from
CREATE PUBLICATION dbz_publication FOR TABLE public.iot_events;

-- A couple of seed rows so downstream consumers have something immediately
INSERT INTO public.iot_events (device_id, latitude, longitude, aqi, temperature_c, battery_pct, severity)
VALUES
    ('device-001', 51.5033, -0.0043, 42.5, 21.3, 87.2, 'normal'),
    ('device-002', 51.5035, -0.0041, 65.1, 22.8, 91.0, 'warning');
