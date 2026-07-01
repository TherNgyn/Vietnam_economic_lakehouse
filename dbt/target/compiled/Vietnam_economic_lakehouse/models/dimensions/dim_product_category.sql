


select
    
    abs(xxhash64(coalesce(cast(product_category_name as string), '__null__')))
 as product_category_key,
    product_category_name
from (
    select distinct product_category_name
    from gold_staging.stg_product_market
    where product_category_name is not null
)