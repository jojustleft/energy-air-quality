SELECT
    cast(country as varchar) as country,
    cast(country_long as varchar) as country_long,
    cast(name as varchar) as power_plant_name,
    cast(gppd_idnr as varchar) as gppd_idnr,
    cast(capacity_mw as numeric) as capacity_mw,
    cast(latitude as double precision) as lat,
    cast(longitude as double precision) as lon,
    cast(primary_fuel as varchar) as primary_fuel
FROM {{ source('openaq_raw', 'power_plants') }}
