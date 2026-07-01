

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        t.month,
        t.quarter,
        f.product_key,
        p.product_name,
        pc.product_category_name,
        f.unit_key,
        u.unit_name,
        f.value,
        f.quantity
    from gold_gold.fact_product_market f
    left join gold_gold.dim_time t
        on f.time_key = t.time_key
    left join gold_gold.dim_product p
        on f.product_key = p.product_key
    left join gold_gold.dim_product_category pc
        on p.product_category_key = pc.product_category_key
    left join gold_gold.dim_unit u
        on f.unit_key = u.unit_key
),

calc as (
    select
        *,
        lag(value) over (
            partition by product_key, unit_key
            order by time_key
        ) as prev_value,

        lag(quantity) over (
            partition by product_key, unit_key
            order by time_key
        ) as prev_quantity,

        sum(value) over (
            partition by time_key, unit_key
        ) as total_value_by_period
    from base
)

select
    *,
    
    value / nullif(quantity, 0)
 as unit_price,

    value - prev_value as value_change,
    
    value - prev_value / nullif(prev_value, 0)
 * 100 as value_growth_pct,

    quantity - prev_quantity as quantity_change,
    
    quantity - prev_quantity / nullif(prev_quantity, 0)
 * 100 as quantity_growth_pct,

    
    value / nullif(total_value_by_period, 0)
 * 100 as product_share_pct
from calc