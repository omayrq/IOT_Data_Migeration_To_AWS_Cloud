-- Ensures that key telemetry fields in the clean silver model are never null.
-- Any returned rows indicate a test failure.

SELECT *
FROM {{ ref('iot_events_clean') }}
WHERE device_id IS NULL
   OR event_timestamp IS NULL
   OR latitude IS NULL
   OR longitude IS NULL
   OR temperature IS NULL
   OR humidity IS NULL
   OR battery IS NULL
