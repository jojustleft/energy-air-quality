-- tests/assert_one_id_per_parameter.sql
-- This tests fails if there are multiple sensor Ids for a given parameter for any air quality location

-- During development, this scenario never occurred, however, we cannot filter it outright because we wouldn't know
-- in which circumstances this happens (Sensor replacement? Measurement redundancy?)
-- The assert_non_overlapping_sensors test will check if the multiple sensors are redundant (i.e.: there is overlap in usage timeline)

{{ config(severity='warn') }}

SELECT
    location_id,
    parameter,
    COUNT(DISTINCT sensors_id) AS total_sensors
FROM {{ ref('stg_measurements') }}
GROUP BY location_id, parameter
HAVING total_sensors > 1
