-- tests/assert_non_negative_values.sql
-- This tests fails if any registered air quality reading is negative

SELECT
    ts_utc,
    location_id,
    measurement
FROM {{ ref('int_measurements') }}
WHERE measurement < 0