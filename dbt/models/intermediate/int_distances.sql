/*
This table keeps a row for every power plant which is within a given distance (here 100km)
of an OpenAQ station.
*/

SELECT
    p.gppd_idnr AS power_plant_id,
    p.primary_fuel AS primary_fuel,
    l.id AS location_id,
    p.lat AS power_plant_lat,
    p.lon AS power_plant_lon,
    l.lat AS location_lat,
    l.lon AS location_lon,
    ST_Distance_Spheroid(p.coord, l.coord) AS distance_m
FROM (
    SELECT 
        gppd_idnr,
        -- Avoid another join when aggregating this table afterwards
        primary_fuel,
        lon,
        lat,
        ST_Point(lat, lon) AS coord
    FROM {{ ref('stg_power_plants') }}
) p
JOIN (
    SELECT 
        id,
        lat,
        lon,
        ST_Point(lat, lon) AS coord
    FROM {{ ref('stg_locations') }}
) l
-- To avoid very large tables, we include power plants within 100 km only
ON ST_DWithin_Spheroid(p.coord, l.coord, 100000)
