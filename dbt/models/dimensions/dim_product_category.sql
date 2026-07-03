{{ config(
    materialized='delta_table'
)}}


select
    {{ sk(['product_category_name']) }} as product_category_key,
    product_category_name
from (
    select distinct product_category_name
    from {{ ref('stg_production_output') }}
    where product_category_name is not null
)