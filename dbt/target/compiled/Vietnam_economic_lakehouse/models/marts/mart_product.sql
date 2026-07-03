

with base as (

    select
        f.fact_production_key,

        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,
        cast(t.quarter as int) as report_quarter,
        cast(t.month as int) as report_month,
        cast(t.day as int) as report_day,

        f.product_key,
        p.product_name,

        f.product_category_key,
        pc.product_category_name,

        f.period_grain,

        cast(f.value as decimal(38,3)) as production_value,
        cast(f.prev_period_value as decimal(38,3)) as production_value_pre_period,
        cast(f.pre_year_value as decimal(38,3)) as production_value_pre_year,

        f.unit as unit_name,
        u.unit_nor,

        f.created_at,
        f.ingest_at

    from gold_gold.fact_production_output f

    left join gold_gold.dim_time t
        on f.time_key = t.time_key

    left join gold_gold.dim_product p
        on f.product_key = p.product_key

    left join gold_gold.dim_product_category pc
        on f.product_category_key = pc.product_category_key

    left join gold_gold.dim_unit u
        on f.unit = u.unit_name

),

with_growth as (

    select
        fact_production_key,

        time_key,
        full_date,
        report_year,
        report_quarter,
        report_month,
        report_day,

        product_key,
        product_name,

        product_category_key,
        product_category_name,

        period_grain,

        production_value,
        production_value_pre_period,
        production_value_pre_year,

        unit_name,
        unit_nor,

        cast(
            round(
                production_value - production_value_pre_period,
                3
            ) as decimal(38,3)
        ) as production_period_change,

        cast(
            round(
                
    production_value - production_value_pre_period / nullif(production_value_pre_period, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as period_growth_rate,

        cast(
            round(
                production_value - production_value_pre_year,
                3
            ) as decimal(38,3)
        ) as production_yoy_change,

        cast(
            round(
                
    production_value - production_value_pre_year / nullif(production_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as yoy_growth_rate,

        created_at,
        ingest_at

    from base

),

with_totals as (

    select
        *,

        sum(production_value) over (
            partition by time_key, period_grain, unit_name
        ) as total_production_value,

        sum(production_value) over (
            partition by time_key, product_category_key, period_grain, unit_name
        ) as category_total_production_value

    from with_growth

),

final as (

    select
        fact_production_key as mart_production_output_key,

        time_key,
        full_date,
        report_year,
        report_quarter,
        report_month,
        report_day,

        product_key,
        product_name,

        product_category_key,
        product_category_name,

        period_grain,

        production_value,
        production_value_pre_period,
        production_value_pre_year,

        production_period_change,
        period_growth_rate,

        production_yoy_change,
        yoy_growth_rate,

        cast(
            round(
                
    production_value / nullif(total_production_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as product_share_pct,

        cast(
            round(
                
    production_value / nullif(category_total_production_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as product_share_in_category_pct,

        unit_name,
        unit_nor,

        created_at,
        ingest_at

    from with_totals

)

select *
from final