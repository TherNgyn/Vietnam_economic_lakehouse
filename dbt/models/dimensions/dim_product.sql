{{ config(
    materialized='delta_table'
)}}

with products as (
    select distinct product_name, product_category_name
    from {{ ref('stg_product_market') }}
    where product_name is not null
)

select
    {{ sk(['p.product_name', 'p.product_category_name']) }} as product_key,
    p.product_name,
    pc.product_category_key
from products p
left join {{ ref('dim_product_category') }} pc
    on p.product_category_name = pc.product_category_name