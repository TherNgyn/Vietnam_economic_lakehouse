{{ config(
    materialized='delta_table'
) }}

with classes as (
    select distinct upper(trim(asset_class_name)) as asset_class_name from {{ ref('stg_ohlc') }}
    union
    select distinct upper(trim(asset_class_name)) as asset_class_name from {{ ref('stg_interest_rate') }}
)

select
    row_number() over (order by asset_class_name) as asset_class_key,
    asset_class_name
from classes
where asset_class_name is not null