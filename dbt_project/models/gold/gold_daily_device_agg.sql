{{
  config(
    materialized = 'table'
  )
}}

-- Gold layer: one row per device per calendar day, aggregated metrics for BI.

select
    device_id,
    date_trunc('day', event_ts)          as event_date,
    count(*)                             as reading_count,
    avg(aqi)                             as avg_aqi,
    max(aqi)                             as max_aqi,
    min(aqi)                             as min_aqi,
    avg(temperature_c)                   as avg_temperature_c,
    avg(battery_pct)                     as avg_battery_pct,
    min(battery_pct)                     as min_battery_pct,
    count_if(severity = 'critical')      as critical_events,
    count_if(severity = 'warning')       as warning_events,
    max(event_ts)                        as last_seen_at,
    current_timestamp()                  as dbt_loaded_at

from {{ ref('silver_iot_events') }}
group by 1, 2
