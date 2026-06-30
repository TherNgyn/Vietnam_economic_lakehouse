fact_product_market.sql{{
    config(
        materialized='incremental',
        unique_key='fact_product_key'
    )
}}

with joined as (
    select
        cast(date_format(p.report_date, 'yyyyMMdd') as int) as time_key,
        dp.product_key,
        u.unit_key,
        s.source_key,
        p.value,
        p.quantity
    from {{ ref('stg_product_market') }} p
    left join {{ ref('dim_product') }} dp
        on p.product_name = dp.product_name
    left join {{ ref('dim_unit') }} u
        on p.unit_name = u.unit_name
    left join {{ ref('dim_source') }} s
        on p.source_name = s.source_name
)

select
    {{ sk(['time_key', 'product_key', 'unit_key', 'source_key']) }} as fact_product_key,
    time_key,
    product_key,
    unit_key,
    source_key,
    value,
    quantity,
    {{ sk(['time_key', 'product_key', 'unit_key', 'source_key']) }} as load_id,
    current_timestamp() as created_at
from joined
where product_key is not null