{{ config(materialized='view') }}

select *
from {{ ref('stg_flights') }}
where cancelled = true
   or diverted = true