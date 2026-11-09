-- This table is useful for debugging potential sensor issues that may lead to spikes in the final results.
-- As an example: location id 3970 registered SO2 values above 1000 µg/m³ during November 2020. Considering that
-- the sensor's activity stops shortly after that, it is probably a malfunction.

SELECT
    location_id,
    sensors_id,
    parameter,
    MIN(ts_utc) AS min_ts_utc,
    MAX(ts_utc) AS max_ts_utc
FROM {{ ref('stg_measurements') }}
GROUP BY location_id, sensors_id, parameter
ORDER BY location_id, sensors_id
