{{ config(materialized='table') }}

select
    dc.carrier_code,
    dc.carrier_name,
    f.flight_year,
    f.flight_month,
    count(*) as total_flights,
    avg(f.dep_delay) as avg_dep_delay,
    avg(f.arr_delay) as avg_arr_delay,
    sum(case when f.arr_delay_15plus then 1 else 0 end) / count(*) as pct_delayed
from {{ ref('fct_flights') }} f
join {{ ref('dim_carriers') }} dc on f.carrier_sk = dc.carrier_sk
group by dc.carrier_code, dc.carrier_name, f.flight_year, f.flight_month