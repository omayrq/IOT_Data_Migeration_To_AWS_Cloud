{% macro check_data_quality() %}
CASE
    WHEN device_id IS NULL              THEN 'NULL_DEVICE_ID'
    WHEN latitude  IS NULL              THEN 'NULL_LATITUDE'
    WHEN longitude IS NULL              THEN 'NULL_LONGITUDE'
    WHEN temperature IS NULL            THEN 'NULL_TEMPERATURE'
    WHEN latitude  NOT BETWEEN -90  AND 90   THEN 'INVALID_LATITUDE'
    WHEN longitude NOT BETWEEN -180 AND 180  THEN 'INVALID_LONGITUDE'
    WHEN temperature NOT BETWEEN {{ var('min_temperature', -40) }} AND {{ var('max_temperature', 80) }}
                                        THEN 'TEMPERATURE_OUT_OF_RANGE'
    WHEN humidity    NOT BETWEEN 0 AND 100   THEN 'HUMIDITY_OUT_OF_RANGE'
    WHEN battery     NOT BETWEEN 0 AND 100   THEN 'BATTERY_OUT_OF_RANGE'
    WHEN event_timestamp IS NULL        THEN 'NULL_TIMESTAMP'
    WHEN event_timestamp > CURRENT_TIMESTAMP()
                                        THEN 'FUTURE_TIMESTAMP'
    WHEN event_timestamp < '2020-01-01'::TIMESTAMP_NTZ
                                        THEN 'TIMESTAMP_TOO_OLD'
    ELSE 'VALID'
END
{% endmacro %}
