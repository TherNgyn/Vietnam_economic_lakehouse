{{ config(
    materialized='delta_table'
)}}


with indicators as (
    select distinct
        lower(trim(indicator_name)) as indicator_name,
        lower(trim(indicator_group_name)) as indicator_group_name
    from {{ ref('stg_macro_indicator') }}
    where indicator_name is not null
)

select
    {{ sk(['i.indicator_name', 'i.indicator_group_name']) }} as indicator_key,
    i.indicator_name,
    g.indicator_group_key
from indicators i
left join {{ ref('dim_indicator_group') }} g
    on i.indicator_group_name = g.indicator_group_name