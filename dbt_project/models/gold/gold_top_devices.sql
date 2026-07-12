{{
  config(
    materialized = 'table'
  )
}}

-- Gold layer: ranks devices by average AQI over the trailing 24 hours.
-- Directly powers the Streamlit "Top-N devices" chart.

with recent as (
    select *
    from {{ ref('silver_iot_events') }}
    where event_ts >= dateadd('hour', -24, current_timestamp())
),

ranked as (
    select
        device_id,
        count(*)                        as reading_count_24h,
        avg(aqi)                        as avg_aqi_24h,
        max(aqi)                        as max_aqi_24h,
        count_if(severity = 'critical') as critical_events_24h,
        avg(latitude)                   as last_latitude,
        avg(longitude)                  as last_longitude,
        rank() over (order by avg(aqi) desc) as aqi_rank
    from recent
    group by device_id
)

select * from ranked
order by aqi_rank
