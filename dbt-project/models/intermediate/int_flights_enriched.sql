{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('stg_flights_completed') }}
),

enriched as (
    select
        base.*,

        -- Cascading delay: this specific aircraft's previous flight's arrival delay
        lag(arr_delay) over (
            partition by tail_number
            order by fl_date, dep_time
        ) as prev_flight_arr_delay,

        -- Congestion: rolling avg departure delay at this origin/hour, trailing 30 days
        -- (excludes the current row itself, so the flight's own outcome never leaks into its own feature)
        avg(dep_delay) over (
            partition by origin, cast(crs_dep_time / 100 as int)
            order by fl_date
            rows between 30 preceding and 1 preceding
        ) as origin_hourly_avg_delay_30d,

        -- Congestion: how many flights are scheduled from this origin, same hour, same day
        count(*) over (
            partition by origin, fl_date, cast(crs_dep_time / 100 as int)
        ) as origin_hourly_flight_count,

        -- Carrier-level historical performance, trailing 90 days
        avg(arr_delay) over (
            partition by carrier_code
            order by fl_date
            rows between 90 preceding and 1 preceding
        ) as carrier_90d_avg_delay,

        -- Route-level historical performance, trailing 90 days
        avg(arr_delay) over (
            partition by origin, dest
            order by fl_date
            rows between 90 preceding and 1 preceding
        ) as route_90d_avg_delay,

        -- Seasonality / calendar flags
        case when day_of_week in (6, 7) then true else false end as is_weekend,
        case when flight_month in (11, 12) then true else false end as is_holiday_season,

        -- ML target column
        case when arr_delay >= 15 then true else false end as arr_delay_15plus

    from base
)

select * from enriched