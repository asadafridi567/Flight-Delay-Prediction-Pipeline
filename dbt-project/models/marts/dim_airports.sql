{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['airport_code', 'dbt_valid_from']) }} as airport_sk,
    airport_code,
    airport_name,
    city,
    country,
    latitude,
    longitude,
    timezone,
    tz_database_timezone,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31')) as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('airports_snapshot') }}