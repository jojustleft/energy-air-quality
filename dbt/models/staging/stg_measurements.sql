WITH 
    measurements AS (
        SELECT *
        FROM {{ source('openaq_raw', 'measurements') }}
    ),
    renamed AS (
        SELECT
            -- This column is the one that should be used for checking for duplicated rows
            timezone('utc', datetime::TIMESTAMPTZ) as ts_utc,
            left(datetime, 19)::TIMESTAMP as ts_local,
            cast(location_id as integer) as location_id,
            cast(sensors_id as integer) as sensors_id,
            cast(location as varchar) as location_name,
            cast(lat as double precision) as lat,
            cast(lon as double precision) as lon,
            cast(parameter as varchar) as parameter,
            cast(units as varchar) as units,
            cast(value as double precision) as measurement,
            cast(created_at as timestamp) as created_at,
            cast(filename as varchar) as filename
        FROM measurements
    )
SELECT * FROM renamed