{{ config(
    materialized='delta_table'
)}}


with sectors as (
    select distinct sector_name from {{ ref('stg_gdp') }}
)

select
    {{ sk(['sector_name']) }} as sector_key,
    sector_name
from sectors
where sector_name is not null