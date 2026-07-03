
        CREATE TABLE gold_marts.mart_gdp_metrics
        USING DELTA
        AS
        

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        t.quarter,
        f.sub_sector_key,
        ss.sub_sector_name,
        s.sector_name,
        f.unit_key,
        u.unit_name,
        f.market_value,
        f.constant_value
    from gold_gold.fact_gdp f
    left join gold_gold.dim_time t
        on f.time_key = t.time_key
    left join gold_gold.dim_sub_sector ss
        on f.sub_sector_key = ss.sub_sector_key
    left join gold_gold.dim_sector s
        on ss.sector_key = s.sector_key
    left join gold_gold.dim_unit u
        on f.unit_key = u.unit_key
),

calc as (
    select
        *,
        lag(market_value) over (
            partition by sub_sector_key, unit_key
            order by time_key
        ) as prev_market_value,

        lag(constant_value) over (
            partition by sub_sector_key, unit_key
            order by time_key
        ) as prev_constant_value,

        sum(market_value) over (
            partition by time_key, unit_key
        ) as total_market_value
    from base
)

select
    *,
    market_value - prev_market_value as market_growth_value,
    
    market_value - prev_market_value / nullif(prev_market_value, 0)
 * 100 as market_growth_pct,

    constant_value - prev_constant_value as real_growth_value,
    
    constant_value - prev_constant_value / nullif(prev_constant_value, 0)
 * 100 as real_growth_pct,

    
    market_value / nullif(total_market_value, 0)
 * 100 as sector_share_pct
from calc
    