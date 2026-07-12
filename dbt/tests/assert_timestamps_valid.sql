-- Ensures that event timestamps in clean silver models are realistic.
-- Rejects event timestamps in the future or older than Jan 1, 2020.
-- Any returned rows indicate a test failure.

SELECT *
FROM {{ ref('iot_events_clean') }}
WHERE event_timestamp > CURRENT_TIMESTAMP()
   OR event_timestamp < '2020-01-01 00:00:00'::TIMESTAMP_NTZ
