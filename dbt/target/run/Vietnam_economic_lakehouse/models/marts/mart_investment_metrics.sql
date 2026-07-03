
        CREATE TABLE gold_marts.mart_investment_metrics
        USING DELTA
        AS
        

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        f.sub_sector_key,
        ss.sub_sector_name,
        s.sector_name,
        f.unit_key,
        u.unit_name,
        f.market_value as investment_value
    from gold_gold.fact_investment f
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
        lag(investment_value) over (
            partition by sub_sector_key, unit_key
            order by time_key
        ) as prev_investment_value,

        sum(investment_value) over (
            partition by time_key, unit_key
        ) as total_investment_value
    from base
)

select
    *,
    investment_value - prev_investment_value as investment_growth_value,
    
    investment_value - prev_investment_value / nullif(prev_investment_value, 0)
 * 100 as investment_growth_pct,
    
    investment_value / nullif(total_investment_value, 0)
 * 100 as investment_share_pct
from calc
    