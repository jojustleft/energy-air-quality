-- tests/assert_one_id_per_parameter.sql
-- This tests fails if there are multiple sensor Ids for a given parameter for any air quality location

-- During development, this scenario never occurred, however, we cannot filter it outright because we wouldn't know
-- in which circumstances this happens (Sensor replacement? Measurement redundancy?)
-- So, it is best that it is handled only if it actually happens.
{{ config(severity='warn') }}

SELECT
    location_id,
    parameter,
    COUNT(DISTINCT sensors_id) AS total_sensors
FROM {{ ref('stg_measurements') }}
GROUP BY location_id, parameter
HAVING total_sensors > 1