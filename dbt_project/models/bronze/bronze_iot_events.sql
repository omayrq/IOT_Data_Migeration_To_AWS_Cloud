{{
  config(
    materialized = 'view'
  )
}}

-- Bronze view: flattens the Snowflake-connector VARIANT payload into typed
-- columns without doing any cleaning/dedup — that happens in Silver.

select
    record_content:device_id::string                          as device_id,
    record_content:latitude::float                             as latitude,
    record_content:longitude::float                            as longitude,
    record_content:aqi::float                                  as aqi,
    record_content:temperature_c::float                        as temperature_c,
    record_content:battery_pct::float                          as battery_pct,
    record_content:severity::string                            as severity,
    record_content:event_ts::string                            as event_ts_raw,
    record_content:__op::string                                as cdc_op,        -- c/u/d
    record_content:__ts_ms::number                             as cdc_ts_ms,
    record_metadata:CreateTime::timestamp_ntz                  as kafka_ingest_ts,
    record_metadata:offset::number                             as kafka_offset,
    record_metadata:partition::number                          as kafka_partition
from {{ source('raw', 'iot_events') }}
