{{ config(
    materialized='delta_table'
)}}



with classes as (
    select distinct asset_class_name from {{ ref('stg_ohlc') }}
    union
    select distinct asset_class_name from {{ ref('stg_interest_rate') }}
)

select
    {{ sk(['asset_class_name']) }} as asset_class_key,
    asset_class_name
from classes
where asset_class_name is not null