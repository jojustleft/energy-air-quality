SELECT id, name, sensor_units, lat, lon, licenses, dt_start, dt_end, created_at
FROM {{ source('openaq_raw', 'locations') }}