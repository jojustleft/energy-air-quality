-- tests/assert_similar_coordinates.sql
-- This tests fails if OpenAQ station coordinates differ considerably between historical and API data

WITH 
    locations AS (SELECT id, lat, lon FROM {{ ref('stg_locations') }}),
    raw_data AS (
        SELECT location_id, lat, lon, COUNT(*) AS num_rows
        FROM {{ ref('stg_measurements') }}
        GROUP BY location_id, lat, lon
    )
SELECT
    location_id,
    m.lat,
    d.lat,
    m.lon,
    d.lon
FROM raw_data m
    JOIN locations d
    ON m.location_id = d.id
WHERE 
    (ABS(m.lat - d.lat) / d.lat) > 0.00001
    OR (ABS(m.lon - d.lon) / d.lon) > 0.00001
ORDER BY location_id