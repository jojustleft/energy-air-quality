SELECT
    id,
    name,
    sensor_units,
    lat,
    lon,
    licenses,
    dt_start,
    dt_end,
    nearby_stations,
    nearby_stations_high_pollution,
    nearby_stations_low_medium_pollution
FROM {{ ref('stg_locations') }} l
LEFT JOIN (
    SELECT
        location_id,
        countIf(distance_m < 10000) as nearby_stations,
        countIf((distance_m < 10000) and (primary_fuel in ('Coal', 'Oil', 'Gas'))) as nearby_stations_high_pollution,
        countIf((distance_m < 10000) and (primary_fuel in ('Waste', 'Biomass', 'Geothermal', 'Hydro'))) as nearby_stations_low_medium_pollution
    FROM {{ ref('int_distances') }}
    GROUP BY location_id
) d
ON l.id = d.location_id
