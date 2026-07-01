

with products as (
    select distinct product_name, product_category_name
    from gold_staging.stg_product_market
    where product_name is not null
)

select
    
    abs(xxhash64(coalesce(cast(p.product_name as string), '__null__'), coalesce(cast(p.product_category_name as string), '__null__')))
 as product_key,
    p.product_name,
    pc.product_category_key
from products p
left join gold_gold.dim_product_category pc
    on p.product_category_name = pc.product_category_name