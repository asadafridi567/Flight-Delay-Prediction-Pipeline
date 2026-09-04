{{ config(materialized='table') }}

select
    {{ dbt_utils.generate_surrogate_key(['carrier_code', 'dbt_valid_from']) }} as carrier_sk,
    carrier_code,
    carrier_name,
    country,
    active,
    dbt_valid_from as valid_from,
    coalesce(dbt_valid_to, timestamp('9999-12-31')) as valid_to,
    case when dbt_valid_to is null then true else false end as is_current
from {{ ref('carriers_snapshot') }}