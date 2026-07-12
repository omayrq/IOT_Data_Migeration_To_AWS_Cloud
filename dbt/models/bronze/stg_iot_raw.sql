{{/*
  stg_iot_raw.sql — Bronze Layer
  ================================
  Parses raw VARIANT JSON from Snowflake Kafka connector.
  The Kafka connector stores:
    RECORD_METADATA: Kafka partition/offset/timestamp metadata
    RECORD_CONTENT:  Full Debezium CDC payload (unwrapped by ExtractNewRecordState)

  This view normalises the raw VARIANT into typed columns.
  No filtering here — that is Silver's job.
*/}}

WITH raw_source AS (
    SELECT
        RECORD_METADATA,
        RECORD_CONTENT,
        INGESTED_AT
    FROM {{ var('raw_database') }}.{{ var('raw_schema') }}.{{ var('raw_table') }}
),

parsed AS (
    SELECT
        -- Kafka metadata
        RECORD_METADATA:CreateTime::BIGINT                              AS kafka_create_time_ms,
        RECORD_METADATA:offset::BIGINT                                  AS kafka_offset,
        RECORD_METADATA:partition::INTEGER                              AS kafka_partition,
        RECORD_METADATA:topic::VARCHAR                                   AS kafka_topic,

        -- Debezium CDC operation metadata
        RECORD_CONTENT:__op::VARCHAR                                    AS cdc_operation,    -- 'r','c','u','d'
        RECORD_CONTENT:__table::VARCHAR                                  AS source_table,
        RECORD_CONTENT:cdc_timestamp_ms::BIGINT                         AS cdc_timestamp_ms,

        -- Core IoT fields
        RECORD_CONTENT:device_id::VARCHAR(50)                           AS device_id,
        RECORD_CONTENT:region::VARCHAR(50)                              AS region,
        RECORD_CONTENT:latitude::DOUBLE                                  AS latitude,
        RECORD_CONTENT:longitude::DOUBLE                                 AS longitude,
        RECORD_CONTENT:temperature::DOUBLE                               AS temperature,
        RECORD_CONTENT:humidity::DOUBLE                                  AS humidity,
        RECORD_CONTENT:battery::DOUBLE                                   AS battery,
        RECORD_CONTENT:speed::DOUBLE                                     AS speed,
        RECORD_CONTENT:signal_strength::INTEGER                          AS signal_strength,
        RECORD_CONTENT:is_online::BOOLEAN                                AS is_online,
        RECORD_CONTENT:event_type::VARCHAR(50)                           AS event_type,

        -- Timestamps
        TO_TIMESTAMP_NTZ(
            RECORD_CONTENT:event_timestamp::VARCHAR
        )                                                               AS event_timestamp,
        INGESTED_AT                                                     AS snowflake_ingested_at,

        -- Full payload preserved for debugging
        RECORD_CONTENT                                                  AS raw_payload

    FROM raw_source
    WHERE RECORD_CONTENT IS NOT NULL
      AND RECORD_CONTENT:device_id IS NOT NULL   -- Skip tombstones / nulls
      AND cdc_operation IN ('r', 'c', 'u')       -- Read, Create, Update only (d=delete handled separately)
)

SELECT * FROM parsed
