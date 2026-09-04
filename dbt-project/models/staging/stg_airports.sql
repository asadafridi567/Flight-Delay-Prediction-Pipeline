{{ config(materialized='view') }}

select
    iata as airport_code,
    trim(name) as airport_name,
    trim(city) as city,
    trim(country) as country,
    latitude,
    longitude,
    timezone,
    tz_database_timezone
from {{ source('bronze', 'airports_lookup') }}
where iata is not null
  and iata != '\\N'
  and type = 'airport'