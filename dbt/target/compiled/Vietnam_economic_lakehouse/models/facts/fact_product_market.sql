

with joined as (
    select
        cast(date_format(to_date(p.report_date), 'yyyyMMdd') as int) as time_key,
        dp.product_key,
        u.unit_key,
        s.source_key,
        p.value,
        p.quantity
    from gold_staging.stg_product_market p
    left join gold_gold.dim_product dp
        on p.product_name = dp.product_name
    left join gold_gold.dim_unit u
        on p.unit_name = u.unit_name
    left join gold_gold.dim_source s
        on p.source_name = s.source_name
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(product_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_product_key,
    time_key,
    product_key,
    unit_key,
    source_key,
    value,
    quantity,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(product_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
    current_timestamp() as created_at
from joined
where product_key is not null