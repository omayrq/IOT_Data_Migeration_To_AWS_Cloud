{{
  config(
    materialized = 'incremental',
    unique_key = 'event_uid',
    on_schema_change = 'append_new_columns'
  )
}}

with bronze as (

    select * from {{ ref('bronze_iot_events') }}
    {% if is_incremental() %}
    where kafka_ingest_ts > (select coalesce(max(kafka_ingest_ts), '1900-01-01') from {{ this }})
    {% endif %}

),

-- CDC can emit multiple events per row (insert, then updates). Keep only the
-- latest version of each event per device+kafka offset, and drop deletes.
deduped as (

    select
        *,
        {{ dbt_utils.generate_surrogate_key(['device_id', 'kafka_partition', 'kafka_offset']) }}
            as event_uid,
        row_number() over (
            partition by device_id, kafka_partition, kafka_offset
            order by kafka_ingest_ts desc
        ) as rn
    from bronze
    where cdc_op is distinct from 'd'   -- drop hard deletes from the fact stream

),

validated as (

    select
        event_uid,
        device_id,
        latitude,
        longitude,
        aqi,
        temperature_c,
        battery_pct,

        -- re-derive severity defensively rather than trusting the source value
        case
            when aqi is null            then 'unknown'
            when aqi >= 100              then 'critical'
            when aqi >= 60               then 'warning'
            else 'normal'
        end as severity,

        try_to_timestamp_ntz(event_ts_raw)  as event_ts,
        kafka_ingest_ts,
        cdc_op,
        current_timestamp()                 as dbt_loaded_at

    from deduped
    where rn = 1
      and device_id is not null
      and latitude between -90 and 90
      and longitude between -180 and 180

)

select * from validated
