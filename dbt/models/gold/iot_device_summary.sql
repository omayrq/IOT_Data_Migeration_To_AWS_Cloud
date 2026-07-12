{{/*
  iot_device_summary.sql — Gold Layer
  ======================================
  Daily KPI aggregations per device.
  Source: Silver iot_events_clean (validated, deduplicated events)
  Rebuilt fully each run (table materialization).

  Metrics:
    - Event count, uptime %, avg/min/max temperature & humidity
    - Battery drain (start vs end of day)
    - Signal quality distribution
    - Active hours count
*/}}

{{
  config(
    materialized = 'table',
    cluster_by   = ['event_date', 'region']
  )
}}

WITH daily_agg AS (
    SELECT
        event_date,
        device_id,
        region,

        -- Volume
        COUNT(*)                                        AS total_events,
        COUNT(DISTINCT HOUR(event_timestamp))           AS active_hours,

        -- Uptime
        ROUND(
            100.0 * SUM(CASE WHEN is_online THEN 1 ELSE 0 END) / COUNT(*), 2
        )                                               AS uptime_pct,

        -- Temperature stats
        ROUND(AVG(temperature), 2)                      AS avg_temperature,
        ROUND(MIN(temperature), 2)                      AS min_temperature,
        ROUND(MAX(temperature), 2)                      AS max_temperature,
        ROUND(STDDEV(temperature), 4)                   AS stddev_temperature,

        -- Humidity stats
        ROUND(AVG(humidity), 2)                         AS avg_humidity,
        ROUND(MIN(humidity), 2)                         AS min_humidity,
        ROUND(MAX(humidity), 2)                         AS max_humidity,

        -- Battery (drain tracking)
        ROUND(FIRST_VALUE(battery) OVER (
            PARTITION BY event_date, device_id
            ORDER BY event_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ), 2)                                           AS battery_start_of_day,
        ROUND(LAST_VALUE(battery) OVER (
            PARTITION BY event_date, device_id
            ORDER BY event_timestamp ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ), 2)                                           AS battery_end_of_day,
        ROUND(MIN(battery), 2)                          AS min_battery,

        -- Signal quality
        ROUND(AVG(signal_strength), 1)                  AS avg_signal_strength,
        SUM(CASE WHEN signal_quality = 'excellent' THEN 1 ELSE 0 END) AS signal_excellent_count,
        SUM(CASE WHEN signal_quality = 'good'      THEN 1 ELSE 0 END) AS signal_good_count,
        SUM(CASE WHEN signal_quality = 'fair'      THEN 1 ELSE 0 END) AS signal_fair_count,
        SUM(CASE WHEN signal_quality = 'poor'      THEN 1 ELSE 0 END) AS signal_poor_count,

        -- Anomaly flags
        MAX(CASE WHEN temperature > 50  THEN 1 ELSE 0 END) AS had_high_temp_alert,
        MAX(CASE WHEN battery    < 10   THEN 1 ELSE 0 END) AS had_critical_battery,
        MAX(CASE WHEN is_online = FALSE THEN 1 ELSE 0 END) AS had_offline_event,

        -- Battery health (worst seen today)
        MIN(battery_health)                             AS worst_battery_health,

        -- Timestamps
        MIN(event_timestamp)                            AS first_event_at,
        MAX(event_timestamp)                            AS last_event_at,
        MAX(snowflake_ingested_at)                      AS last_updated_at

    FROM {{ ref('iot_events_clean') }}
    GROUP BY event_date, device_id, region
)

SELECT
    event_date,
    device_id,
    region,
    total_events,
    active_hours,
    uptime_pct,
    avg_temperature,
    min_temperature,
    max_temperature,
    stddev_temperature,
    avg_humidity,
    min_humidity,
    max_humidity,
    battery_start_of_day,
    battery_end_of_day,
    ROUND(battery_start_of_day - battery_end_of_day, 2) AS battery_drain_today,
    min_battery,
    avg_signal_strength,
    signal_excellent_count,
    signal_good_count,
    signal_fair_count,
    signal_poor_count,
    had_high_temp_alert,
    had_critical_battery,
    had_offline_event,
    worst_battery_health,
    first_event_at,
    last_event_at,
    last_updated_at,
    -- Composite health score (0–100)
    ROUND(
        LEAST(100, GREATEST(0,
            (uptime_pct * 0.4) +
            (LEAST(min_battery / 100.0, 1.0) * 30) +
            ((signal_excellent_count + signal_good_count) * 1.0 / NULLIF(total_events, 0) * 30)
        )), 1
    )                                                   AS device_health_score
FROM daily_agg
