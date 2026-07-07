{{ config(
    materialized='delta_table'
) }}

with production_products as (
    select distinct
        product_name,
        'not available' as product_type,
        product_category_name
    from {{ ref('stg_production_output') }}
    where product_name is not null
),

trade_products as (
    select distinct
        product_name,
        product_type,
        'Trade International Product' as product_category_name
    from {{ ref('stg_trade_international') }}
    where product_name is not null
),

all_products_unioned as (
    select * from production_products
    union all
    select * from trade_products
),

final_dedup as (
    select distinct
        trim(product_name) as product_name,
        trim(product_type) as product_type,
        trim(product_category_name) as product_category_name
    from all_products_unioned
)

select
    -- Tạo Surrogate Key duy nhất cho toàn bộ hệ thống sản phẩm
    row_number() over (order by product_name, product_type, product_category_name) as product_key,
    product_name,
    product_type,
    product_category_name
from final_dedup