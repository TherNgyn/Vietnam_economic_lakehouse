{{ config(
    materialized='delta_table'
)}}


with sub_sectors as (
    select distinct sub_sector_name
    from {{ ref('stg_gdp') }}
    where sub_sector_name is not null
)

select
    {{ sk(['sub_sector_name']) }} as sub_sector_key,
    sub_sector_name
from sub_sectors