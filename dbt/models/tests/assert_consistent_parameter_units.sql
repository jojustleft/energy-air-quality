-- tests/assert_consistent_parameter_units.sql
-- This tests fails if any parameter (across all locations and timestamps) registers more than one unique unit type

SELECT 
    parameter,
    COUNT(DISTINCT units) AS nunique_units
FROM {{ ref('stg_measurements') }}
GROUP BY parameter
HAVING nunique_units > 1