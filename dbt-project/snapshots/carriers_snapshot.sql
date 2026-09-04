{% snapshot carriers_snapshot %}
{{
    config(
      target_schema='silver',
      unique_key='carrier_code',
      strategy='check',
      check_cols=['carrier_name', 'country', 'active']
    )
}}
select * from {{ ref('stg_carriers') }}
{% endsnapshot %}