{{ config(
    materialized='delta_table'
) }}

with fact as (
    select f.*, dp.product_category_name 
    from {{ ref('fact_production_output') }} f
    left join {{ ref('dim_product') }} dp 
        on f.product_key = dp.product_key
),

calculations as (
    select
        time_key,
        product_key,
        period_grain,
        value,
        unit,
        prev_period_value,
        pre_year_value,

        case 
            when prev_period_value > 0 
            then round(((value - prev_period_value) / prev_period_value) * 100, 3)
        end as period_growth_rate,

        case 
            when pre_year_value > 0 
            then round(((value - pre_year_value) / pre_year_value) * 100, 3)
        end as yoy_growth_rate,

        -- Tính tỷ trọng dựa trên category lấy từ bảng Dim tổng hợp
        round(
            (value / nullif(sum(value) over (partition by report_year, report_quarter, report_month, product_category_name, period_grain), 0)) * 100, 3
        ) as product_share_pct,
        
        ingest_at
    from fact
),

final as (
    select
        {{ sk(['time_key', 'product_key', 'period_grain']) }} as mart_production_key,
        time_key,
        product_key,
        period_grain,
        value,
        unit,
        prev_period_value,
        pre_year_value,
        cast(coalesce(period_growth_rate, 0) as float) as period_growth_rate,
        cast(coalesce(yoy_growth_rate, 0) as float) as yoy_growth_rate,
        cast(coalesce(product_share_pct, 0) as float) as product_share_pct,
        current_timestamp() as created_at,
        ingest_at
    from calculations
)

select * from final