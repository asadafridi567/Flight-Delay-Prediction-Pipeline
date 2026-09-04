{{ config(materialized='view') }}

select
    Year                                as flight_year,
    Quarter                             as flight_quarter,
    Month                               as flight_month,
    DayofMonth                          as day_of_month,
    DayOfWeek                           as day_of_week,
    FlightDate                          as fl_date,

    trim(Reporting_Airline)             as carrier_code,
    trim(Tail_Number)                   as tail_number,
    Flight_Number_Reporting_Airline     as flight_number,

    trim(Origin)                        as origin,
    trim(Dest)                          as dest,

    CRSDepTime                          as crs_dep_time,
    DepTime                             as dep_time,
    DepDelay                            as dep_delay,

    CRSArrTime                          as crs_arr_time,
    ArrTime                             as arr_time,
    ArrDelay                            as arr_delay,

    cast(Cancelled as boolean)          as cancelled,
    trim(CancellationCode)              as cancellation_code,
    cast(Diverted as boolean)           as diverted,

    CRSElapsedTime                      as crs_elapsed_time,
    ActualElapsedTime                   as actual_elapsed_time,
    AirTime                             as air_time,
    Distance                            as distance,

    coalesce(CarrierDelay, 0)           as carrier_delay,
    coalesce(WeatherDelay, 0)           as weather_delay,
    coalesce(NASDelay, 0)               as nas_delay,
    coalesce(SecurityDelay, 0)          as security_delay,
    coalesce(LateAircraftDelay, 0)      as late_aircraft_delay

from {{ source('bronze', 'flights_raw') }}
where FlightDate is not null
  and Reporting_Airline is not null