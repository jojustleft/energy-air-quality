/*
When decreasing time granularity, air quality readings cannot be summed, but rather aggregated into means, percentiles, min and max.
This table is built using daily granularity data, because we want to filter out days that have less than 75% of measurements (18 hours).
*/
SELECT
    date_trunc('month', date_utc)::DATE AS month_utc,
    location_id,
    units,
    parameter,
    MIN(min) AS lowest_min,
    AVG(min) AS mean_min,
    AVG(p25) AS mean_p25,
    AVG(p50) AS mean_p50,
    AVG(p75) AS mean_p75,
    AVG(max) AS mean_max,
    MAX(max) AS highest_max,
    COUNT (DISTINCT date_utc) FILTER (WHERE p50 IS NOT NULL) AS logged_days
FROM {{ ref('int_aq_daily') }}
WHERE
    logged_hours >= 18
GROUP BY ts_month, location_id, units, parameter
ORDER BY ts_month, location_id
