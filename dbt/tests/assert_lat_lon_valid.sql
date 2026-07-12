-- Ensures that latitude and longitude coordinates are physically valid.
-- Latitude must be between -90 and 90 degrees.
-- Longitude must be between -180 and 180 degrees.
-- Any returned rows indicate a test failure.

SELECT *
FROM {{ ref('iot_events_clean') }}
WHERE latitude < -90 OR latitude > 90
   OR longitude < -180 OR longitude > 180
