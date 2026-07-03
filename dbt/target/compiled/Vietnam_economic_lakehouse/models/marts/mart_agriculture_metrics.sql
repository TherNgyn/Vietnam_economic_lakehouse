

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
    from gold_gold.fact_agriculture f
    left join gold_gold.dim_time t
        on f.time_key = t.time_key
    left join gold_gold.dim_crop c
        on f.crop_key = c.crop_key
    left join gold_gold.dim_crop_group cg
        on c.crop_group_key = cg.crop_group_key
),

calc as (
    select
        *,
        
    production / nullif(area, 0)
 as yield_value,
        lag(production) over (partition by crop_key order by time_key) as prev_production,
        lag(area) over (partition by crop_key order by time_key) as prev_area
    from base
)

select
    *,
    production - prev_production as production_change,
    
    production - prev_production / nullif(prev_production, 0)
 * 100 as production_growth_pct,

    area - prev_area as area_change,
    
    area - prev_area / nullif(prev_area, 0)
 * 100 as area_growth_pct
from calc