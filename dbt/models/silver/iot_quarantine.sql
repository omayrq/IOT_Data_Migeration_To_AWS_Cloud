{{/*
  iot_quarantine.sql — Silver Layer (Quarantine)
  ================================================
  Captures records that failed data quality checks.
  Used by ops team to investigate data pipeline issues.
*/}}

{{
  config(
    materialized = 'incremental',
    unique_key   = ['device_id', 'event_timestamp', 'kafka_offset'],
    on_schema_change = 'sync_all_columns'
  )
}}

WITH source AS (
    SELECT * FROM {{ ref('stg_iot_raw') }}

    {% if is_incremental() %}
    WHERE snowflake_ingested_at > (
        SELECT COALESCE(MAX(snowflake_ingested_at), '1900-01-01'::TIMESTAMP_NTZ)
        FROM {{ this }}
    )
    {% endif %}
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY device_id, event_timestamp
            ORDER BY kafka_offset DESC
        ) AS rn
    FROM source
),

validated AS (
    SELECT *,
        {{ check_data_quality() }} AS dq_status
    FROM deduped WHERE rn = 1
)

SELECT
    device_id,
    event_timestamp,
    kafka_offset,
    kafka_partition,
    dq_status                           AS rejection_reason,
    raw_payload                         AS original_payload,
    snowflake_ingested_at               AS quarantined_at,
    CURRENT_TIMESTAMP()                 AS reviewed_at,
    FALSE                               AS is_resolved
FROM validated
WHERE dq_status != 'VALID'
