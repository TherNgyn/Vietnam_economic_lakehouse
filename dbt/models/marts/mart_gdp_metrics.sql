{{ config(materialized='delta_table') }}

with fact as (
    select * from {{ ref('fact_gdp_growth') }}
),

calculations as (
    select
        time_key,
        sub_sector_key,
        unit,
        market_value,
        constant_value,
        market_value_pre_quarter,
        market_value_pre_year,
        constant_value_pre_quarter,
        constant_value_pre_year,

        -- 1. Tính toán Tốc độ tăng trưởng Thị trường (Market Growth Rates) QoQ / YoY
        case 
            when market_value_pre_quarter > 0 
            then round(((market_value - market_value_pre_quarter) / market_value_pre_quarter) * 100, 3) 
        end as market_qoq_growth_rate,
        
        case 
            when market_value_pre_year > 0 
            then round(((market_value - market_value_pre_year) / market_value_pre_year) * 100, 3) 
        end as market_yoy_growth_rate,

        -- 2. Tính toán Tốc độ tăng trưởng Thực tế (Real Growth Rates) QoQ / YoY
        case 
            when constant_value_pre_quarter > 0 
            then round(((constant_value - constant_value_pre_quarter) / constant_value_pre_quarter) * 100, 3) 
        end as real_qoq_growth_rate,
        
        case 
            when constant_value_pre_year > 0 
            then round(((constant_value - constant_value_pre_year) / constant_value_pre_year) * 100, 3) 
        end as real_yoy_growth_rate,

        -- 3. Chỉ số giảm phát GDP (Implicit Price Deflator)
        case 
            when constant_value > 0 
            then round((market_value / constant_value) * 100, 3) 
        end as implicit_price_deflator,
        
        -- 4. Tỷ trọng đóng góp của phân ngành trong toàn bộ Ngành (Sector Share)
        round(
            (market_value / nullif(sum(market_value) over (partition by sector_key, time_key), 0)) * 100, 3
        ) as sector_share_pct,

        -- 5. Tỷ trọng đóng góp của phân ngành vào quy mô tổng thể GDP quốc gia (GDP Share)
        round(
            (market_value / nullif(sum(market_value) over (partition by year, quarter), 0)) * 100, 3
        ) as gdp_share_pct

    from fact
),

final as (
    select
        time_key,
        sub_sector_key,
        unit,
        market_value,
        constant_value,
        market_value_pre_quarter,
        market_value_pre_year,
        constant_value_pre_quarter,
        constant_value_pre_year,
        -- Khử các giá trị Null về 0 và ép kiểu float chính xác như PySpark .fillna(0)
        cast(coalesce(market_qoq_growth_rate, 0) as float) as market_qoq_growth_rate,
        cast(coalesce(market_yoy_growth_rate, 0) as float) as market_yoy_growth_rate,
        cast(coalesce(real_qoq_growth_rate, 0) as float) as real_qoq_growth_rate,
        cast(coalesce(real_yoy_growth_rate, 0) as float) as real_yoy_growth_rate,
        cast(coalesce(implicit_price_deflator, 0) as float) as implicit_price_deflator,
        cast(coalesce(sector_share_pct, 0) as float) as sector_share_pct,
        cast(coalesce(gdp_share_pct, 0) as float) as gdp_share_pct
    from calculations
)

select * from final