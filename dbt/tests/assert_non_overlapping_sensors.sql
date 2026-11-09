-- tests/assert_non_overlapping_sensors.sql
-- This tests fails if there are multiple sensor Ids for a given parameter for any air quality location *with overlap in the usage timeline*.

WITH 
    multiple_sensors AS (
        SELECT
            location_id,
            parameter,
            COUNT(DISTINCT sensors_id) AS total_sensors
        FROM {{ ref('stg_measurements') }}
        GROUP BY location_id, parameter
        HAVING total_sensors > 1
    ),
    sensor_ranges AS (
        SELECT
            location_id,
            parameter,
            sensors_id,
            MIN(ts_utc) AS min_ts,
            MAX(ts_utc) AS max_ts
        FROM {{ ref('stg_measurements') }}
        WHERE 
            location_id IN (SELECT location_id FROM multiple_sensors)
            AND parameter IN (SELECT parameter FROM multiple_sensors)
        GROUP BY location_id, parameter, sensors_id
    )
SELECT
    *
FROM sensor_ranges a
    INNER JOIN sensor_ranges b
    ON (a.location_id = b.location_id AND a.parameter = b.parameter AND a.sensors_id != b.sensors_id)
WHERE
    a.max_ts > b.min_ts AND a.min_ts < b.min_ts
