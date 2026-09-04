{% snapshot airports_snapshot %}
{{
    config(
      target_schema='silver',
      unique_key='airport_code',
      strategy='check',
      check_cols=['airport_name', 'city', 'country', 'timezone']
    )
}}
select * from {{ ref('stg_airports') }}
{% endsnapshot %}