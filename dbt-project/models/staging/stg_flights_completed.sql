{{ config(materialized='view') }}

select *
from {{ ref('stg_flights') }}
where cancelled = false
  and diverted = false