
        CREATE TABLE gold_marts.mart_crop_metrics
        USING DELTA
        AS
        

with fact as (
    select * from gold_gold.fact_crop_yield
),

calculations as (
    select
        time_key,
        crop_key,
        productivity_unit,
        yield_unit,
        area_unit,
        area,
        yield_value,
        productivity,
        area_pre_year,
        yield_pre_year,
        productivity_pre_year,

        -- 1. Tính toán Tốc độ tăng trưởng liên năm (YoY Growth Rate)
        case 
            when area_pre_year > 0 
            then round(((area - area_pre_year) / area_pre_year) * 100, 3) 
        end as area_yoy_growth_rate,

        case 
            when yield_pre_year > 0 
            then round(((yield_value - yield_pre_year) / yield_pre_year) * 100, 3) 
        end as yield_yoy_growth_rate,

        case 
            when productivity > 0 
            then round(((productivity - productivity_pre_year) / productivity_pre_year) * 100, 3) 
        end as productivity_yoy_growth_rate,

        -- 2. Tính toán Tỷ trọng năng suất theo nhóm cây và năm 
        round(
            (productivity / nullif(sum(productivity) over (partition by report_year, crop_category), 0)) * 100, 3
        ) as productivity_share_pct

    from fact
),

final as (
    select
        time_key,
        crop_key,
        productivity_unit,
        yield_unit,
        area_unit,
        area,
        yield_value,
        productivity,
        area_pre_year,
        yield_pre_year,
        productivity_pre_year,
        cast(coalesce(area_yoy_growth_rate, 0) as float) as area_yoy_growth_rate,
        cast(coalesce(yield_yoy_growth_rate, 0) as float) as yield_yoy_growth_rate,
        cast(coalesce(productivity_yoy_growth_rate, 0) as float) as productivity_yoy_growth_rate,
        cast(coalesce(productivity_share_pct, 0) as float) as productivity_share_pct
    from calculations
)

select * from final
    