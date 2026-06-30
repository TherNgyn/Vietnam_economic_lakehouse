{{ config(materialized='table') }}

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        f.crop_key,
        c.crop_name,
        cg.crop_group_name,
        f.production,
        f.area
    from {{ ref('fact_agriculture') }} f
    left join {{ ref('dim_time') }} t
        on f.time_key = t.time_key
    left join {{ ref('dim_crop') }} c
        on f.crop_key = c.crop_key
    left join {{ ref('dim_crop_group') }} cg
        on c.crop_group_key = cg.crop_group_key
),

calc as (
    select
        *,
        {{ safe_divide('production', 'area') }} as yield_value,
        lag(production) over (partition by crop_key order by time_key) as prev_production,
        lag(area) over (partition by crop_key order by time_key) as prev_area
    from base
)

select
    *,
    production - prev_production as production_change,
    {{ safe_divide('production - prev_production', 'prev_production') }} * 100 as production_growth_pct,

    area - prev_area as area_change,
    {{ safe_divide('area - prev_area', 'prev_area') }} * 100 as area_growth_pct
from calc