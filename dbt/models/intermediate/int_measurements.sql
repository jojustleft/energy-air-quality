/*
In the intermediate layer, we filter out rows that are duplicates or have missing important information.

Data was kept in long format, since aggregations into daily and potentially other time frequencies will
result in extra columns (quantiles, min, max, standard deviation, ...). Depending on the amount of 
measurement types, this could result in multiple douzens of columns being added. 

Other fields are removed:
 - location name: differs from location info
 - sensors_id: likely not needed
 - lat and lon: assert_similar_coordinates test ensures these don't differ considerably from API info
    - TODO: A future update should store isMobile on the dimension table as well (these cases could cause the test to fail)
*/
WITH
    stg_measurements AS (
        SELECT *
        FROM {{ ref('stg_measurements') }}
    ),
    time_boundaries AS (
        SELECT
            MIN(ts_utc) AS min_ts,
            MAX(ts_utc) AS max_ts
        FROM {{ ref('stg_measurements') }}
    ),
    time_series AS (
        SELECT ts_utc
        FROM time_boundaries,
        generate_series(time_boundaries.min_ts, time_boundaries.max_ts, INTERVAL 1 HOUR) tbl(ts_utc)
    ),
    breakdown_vals AS (
        SELECT DISTINCT location_id, parameter, units FROM staging.stg_measurements
    ),
    -- This method of padding missing row combinations forces us to select a time column
    -- (Unless we want to increase the query complexity considerably)
    comb_grid AS (SELECT * FROM time_series CROSS JOIN breakdown_vals)
SELECT
    c.ts_utc,
    c.location_id,
    c.parameter,
    c.units,
    CASE WHEN measurement < 0 THEN 0 ELSE measurement END AS measurement
FROM stg_measurements m
    RIGHT JOIN comb_grid c
    ON (m.ts_utc = c.ts_utc AND m.location_id = c.location_id AND m.parameter = c.parameter AND m.units = c.units)
WHERE
    (c.location_id, c.ts_utc, c.parameter, c.units) is not null
-- No duplicated location, parameter, time entries
-- Note: UTC timestamp is being used as it is not impacted by time changes
QUALIFY COUNT(*) OVER (PARTITION BY c.location_id, c.parameter, c.ts_utc) = 1
ORDER BY c.ts_utc, c.location_id
