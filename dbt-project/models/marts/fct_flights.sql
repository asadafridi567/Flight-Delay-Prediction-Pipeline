{{ config(materialized='table') }}

select
    f.flight_year,
    f.flight_month,
    f.day_of_month,
    f.day_of_week,
    f.fl_date,
    f.flight_number,
    f.tail_number,

    dc.carrier_sk,
    da_origin.airport_sk as origin_airport_sk,
    da_dest.airport_sk   as dest_airport_sk,

    f.crs_dep_time,
    f.dep_time,
    f.dep_delay,
    f.crs_arr_time,
    f.arr_time,
    f.arr_delay,
    f.air_time,
    f.distance,

    f.prev_flight_arr_delay,
    f.origin_hourly_avg_delay_30d,
    f.origin_hourly_flight_count,
    f.carrier_90d_avg_delay,
    f.route_90d_avg_delay,
    f.is_weekend,
    f.is_holiday_season,

    f.arr_delay_15plus

from {{ ref('int_flights_enriched') }} f

-- fct_flights.sql (updated joins only)
left join {{ ref('dim_carriers') }} dc
    on f.carrier_code = dc.carrier_code
    and dc.is_current = true

left join {{ ref('dim_airports') }} da_origin
    on f.origin = da_origin.airport_code
    and da_origin.is_current = true

left join {{ ref('dim_airports') }} da_dest
    on f.dest = da_dest.airport_code
    and da_dest.is_current = true