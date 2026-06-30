{{ config(materialized='table') }}

with crops as (
    select distinct crop_name, crop_group_name
    from {{ ref('stg_agriculture') }}
    where crop_name is not null
)

select
    {{ sk(['c.crop_name', 'c.crop_group_name']) }} as crop_key,
    c.crop_name,
    g.crop_group_key
from crops c
left join {{ ref('dim_crop_group') }} g
    on c.crop_group_name = g.crop_group_name