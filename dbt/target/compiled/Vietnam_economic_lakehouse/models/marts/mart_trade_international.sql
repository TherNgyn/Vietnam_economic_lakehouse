

with fact as (
    select * from gold_gold.fact_international_trade
),

calculations as (
    select
        time_key,
        product_key,
        trade_value,
        value_unit,
        quantity,
        quantity_unit,
        trade_value_pre_month,
        trade_value_pre_year,

        -- 1. Tốc độ tăng trưởng liên tháng (MoM Growth Rate)
        case 
            when trade_value_pre_month > 0 
            then round(((trade_value - trade_value_pre_month) / trade_value_pre_month) * 100, 3) 
        end as mom_growth_rate,

        -- 2. Tốc độ tăng trưởng cùng kỳ năm trước (YoY Growth Rate)
        case 
            when trade_value_pre_year > 0 
            then round(((trade_value - trade_value_pre_year) / trade_value_pre_year) * 100, 3) 
        end as yoy_growth_rate,

        -- 3. Tỷ trọng đóng góp giá trị của sản phẩm theo chu kỳ thời gian (Product Share)
        round(
            (trade_value / nullif(sum(trade_value) over (partition by year, quarter, month), 0)) * 100, 3
        ) as product_share_pct,
        
        ingest_at

    from fact
),

final as (
    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(product_key as string), '__null__')))
 as mart_trade_key,
        time_key,
        product_key,
        trade_value,
        value_unit,
        quantity,
        quantity_unit,
        trade_value_pre_month,
        trade_value_pre_year,
        
        -- Khử giá trị rỗng về 0 và định kiểu dữ liệu Float chính xác như PySpark
        cast(coalesce(mom_growth_rate, 0) as float) as mom_growth_rate,
        cast(coalesce(yoy_growth_rate, 0) as float) as yoy_growth_rate,
        cast(coalesce(product_share_pct, 0) as float) as product_share_pct,
        
        current_timestamp() as created_at,
        ingest_at
    from calculations
)

select * from final