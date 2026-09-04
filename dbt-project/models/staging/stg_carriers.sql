{{ config(materialized='view') }}

with base as (
    select
        iata as carrier_code,
        trim(name) as carrier_name,
        trim(country) as country,
        active
    from {{ source('bronze', 'carriers_lookup') }}
    where iata is not null
      and iata != '\\N'
),

deduped as (
    select
        *,
        row_number() over (
            partition by carrier_code
            order by case when active = 'Y' then 0 else 1 end, carrier_name
        ) as rn
    from base
)

select carrier_code, carrier_name, country, active
from deduped
where rn = 1