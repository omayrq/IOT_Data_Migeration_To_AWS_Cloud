{{/*
  iot_events_clean.sql — Silver Layer
  =====================================
  Cleans, deduplicates, and validates raw CDC events.
  Materialized as INCREMENTAL — only processes new Kafka offsets.

  Rules applied:
    1. Deduplicate: keep latest CDC event per (device_id, event_timestamp)
    2. Drop records with null required fields
    3. Validate latitude/longitude bounds
    4. Validate temperature/humidity/battery ranges
    5. Standardize timestamp to UTC
    6. Route invalid records to iot_quarantine (separate model)
    7. Add data quality flags
*/}}

{{
  config(
    materialized       = 'incremental',
    unique_key         = ['device_id', 'event_timestamp'],
    incremental_strategy = 'merge',
    cluster_by         = ['event_date', 'region'],
    on_schema_change   = 'sync_all_columns'
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_iot_raw') }}

    {% if is_incremental() %}
    -- Only load records not yet processed (based on Kafka offset)
    WHERE snowflake_ingested_at > (
        SELECT COALESCE(MAX(snowflake_ingested_at), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

-- Step 1: Deduplicate — keep the latest CDC operation per device+timestamp
deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, event_timestamp
            ORDER BY kafka_offset DESC, cdc_timestamp_ms DESC
        ) AS rn
    FROM source
),

-- Step 2: Apply data quality checks
validated AS (
    SELECT
        device_id,
        region,
        CONVERT_TIMEZONE('UTC', event_timestamp)::TIMESTAMP_NTZ  AS event_timestamp,
        DATE_TRUNC('day', event_timestamp)::DATE                  AS event_date,
        temperature,
        humidity,
        battery,
        speed,
        signal_strength,
        latitude,
        longitude,
        is_online,
        event_type,
        cdc_operation,
        source_table,
        kafka_offset,
        kafka_partition,
        snowflake_ingested_at,

        {{ check_data_quality() }} AS dq_status,

        -- Battery health classification
        CASE
            WHEN battery < {{ var('min_battery_critical') }} THEN 'critical'
            WHEN battery < {{ var('min_battery_warn') }}     THEN 'warning'
            ELSE 'healthy'
        END AS battery_health,

        -- Signal quality classification
        CASE
            WHEN signal_strength > -50  THEN 'excellent'
            WHEN signal_strength > -70  THEN 'good'
            WHEN signal_strength > -85  THEN 'fair'
            ELSE 'poor'
        END AS signal_quality

    FROM deduped
    WHERE rn = 1  -- One record per device+timestamp after dedup
)

-- Step 3: Only promote VALID records to Silver
--         Invalid records go to iot_quarantine (sibling model)
SELECT * FROM validated
WHERE dq_status = 'VALID'
