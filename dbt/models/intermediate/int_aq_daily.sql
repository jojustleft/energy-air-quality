SELECT
    ts_utc::DATE AS date_utc,
    location_id,
    units,
    parameter,
    MIN(measurement) AS min,
    quantile_cont(measurement, 0.25) AS p25,
    MEDIAN(measurement) AS p50,
    AVG(measurement) AS mean,
    quantile_cont(measurement, 0.75) AS p75,
    quantile_cont(measurement, 0.95) AS p95,
    MAX(measurement) AS max,
    COUNT (DISTINCT ts_utc) FILTER (WHERE measurement IS NOT NULL) AS logged_hours
FROM {{ ref('int_measurements') }}
GROUP BY date_utc, location_id, units, parameter
ORDER BY date_utc, location_id