{{ config(materialized='table') }}

select
    da_o.airport_code as origin,
    da_d.airport_code as dest,
    f.flight_year,
    f.flight_month,
    count(*) as total_flights,
    avg(f.arr_delay) as avg_arr_delay,
    avg(f.origin_hourly_avg_delay_30d) as avg_congestion_score
from {{ ref('fct_flights') }} f
join {{ ref('dim_airports') }} da_o on f.origin_airport_sk = da_o.airport_sk
join {{ ref('dim_airports') }} da_d on f.dest_airport_sk = da_d.airport_sk
group by da_o.airport_code, da_d.airport_code, f.flight_year, f.flight_month