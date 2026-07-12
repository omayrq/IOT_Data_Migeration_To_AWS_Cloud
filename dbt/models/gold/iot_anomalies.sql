{{/*
  iot_anomalies.sql — Gold Layer
  ================================
  Real-time anomaly detection using statistical thresholds.
  Each row = one anomaly event that triggered an alert condition.

  Anomaly types:
    - TEMP_SPIKE: temperature 3 standard deviations above device's 7-day mean
    - BATTERY_CRITICAL: battery below 10%
    - BATTERY_WARNING: battery below 20%
    - DEVICE_OFFLINE: is_online = FALSE
    - SIGNAL_LOST: signal_strength below -90 dBm
    - RAPID_TEMP_CHANGE: >10°C change within 5 minutes of prior reading
*/}}

{{
  config(
    materialized = 'table',
    cluster_by   = ['event_date', 'anomaly_type']
  )
}}

WITH base AS (
    SELECT
        device_id,
        region,
        event_date,
        event_timestamp,
        temperature,
        humidity,
        battery,
        signal_strength,
        is_online,
        -- Lag for change detection
        LAG(temperature)    OVER (PARTITION BY device_id ORDER BY event_timestamp) AS prev_temperature,
        LAG(event_timestamp)OVER (PARTITION BY device_id ORDER BY event_timestamp) AS prev_event_timestamp
    FROM {{ ref('iot_events_clean') }}
),

-- 7-day rolling stats for temperature spike detection
device_stats AS (
    SELECT
        device_id,
        event_date,
        AVG(temperature) OVER (
            PARTITION BY device_id
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_avg_temp,
        STDDEV(temperature) OVER (
            PARTITION BY device_id
            ORDER BY event_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_stddev_temp
    FROM {{ ref('iot_device_summary') }}
),

events_with_stats AS (
    SELECT
        b.*,
        ds.rolling_7d_avg_temp,
        ds.rolling_7d_stddev_temp,
        DATEDIFF('minute', b.prev_event_timestamp, b.event_timestamp) AS minutes_since_last
    FROM base b
    LEFT JOIN device_stats ds
        ON b.device_id = ds.device_id
        AND b.event_date = ds.event_date
),

anomalies AS (
    -- Temperature spike (3-sigma rule)
    SELECT
        device_id, region, event_date, event_timestamp,
        'TEMP_SPIKE'                                     AS anomaly_type,
        'critical'                                       AS severity,
        temperature                                      AS metric_value,
        rolling_7d_avg_temp + (3 * rolling_7d_stddev_temp) AS threshold_value,
        'Temperature ' || temperature || '°C exceeds 3σ threshold of ' ||
            ROUND(rolling_7d_avg_temp + 3 * rolling_7d_stddev_temp, 1) || '°C' AS description
    FROM events_with_stats
    WHERE temperature > rolling_7d_avg_temp + (3 * NULLIF(rolling_7d_stddev_temp, 0))
      AND rolling_7d_stddev_temp IS NOT NULL

    UNION ALL

    -- Battery critical
    SELECT device_id, region, event_date, event_timestamp,
        'BATTERY_CRITICAL', 'critical',
        battery, 10.0,
        'Battery critical at ' || battery || '%'
    FROM events_with_stats WHERE battery < 10

    UNION ALL

    -- Battery warning
    SELECT device_id, region, event_date, event_timestamp,
        'BATTERY_WARNING', 'warning',
        battery, 20.0,
        'Battery low at ' || battery || '%'
    FROM events_with_stats WHERE battery BETWEEN 10 AND 20

    UNION ALL

    -- Device offline
    SELECT device_id, region, event_date, event_timestamp,
        'DEVICE_OFFLINE', 'warning',
        0, 1,
        'Device reported offline'
    FROM events_with_stats WHERE is_online = FALSE

    UNION ALL

    -- Signal lost
    SELECT device_id, region, event_date, event_timestamp,
        'SIGNAL_LOST', 'warning',
        signal_strength::DOUBLE, -90.0,
        'Signal strength ' || signal_strength || ' dBm (below -90 threshold)'
    FROM events_with_stats WHERE signal_strength < -90

    UNION ALL

    -- Rapid temperature change (>10°C in ≤5 min)
    SELECT device_id, region, event_date, event_timestamp,
        'RAPID_TEMP_CHANGE', 'warning',
        ABS(temperature - prev_temperature), 10.0,
        'Temperature changed ' || ROUND(ABS(temperature - prev_temperature), 1) ||
            '°C in ' || minutes_since_last || ' minutes'
    FROM events_with_stats
    WHERE ABS(temperature - COALESCE(prev_temperature, temperature)) > 10
      AND minutes_since_last <= 5
      AND prev_temperature IS NOT NULL
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['device_id', 'event_timestamp', 'anomaly_type']) }} AS anomaly_id,
    device_id,
    region,
    event_date,
    event_timestamp,
    anomaly_type,
    severity,
    metric_value,
    threshold_value,
    description,
    CURRENT_TIMESTAMP() AS detected_at
FROM anomalies
ORDER BY event_timestamp DESC
