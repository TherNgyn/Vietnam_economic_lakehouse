

with base as (

    select
        f.fact_trade_key,

        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,
        cast(t.quarter as int) as report_quarter,
        cast(t.month as int) as report_month,

        f.product_key,
        p.product_name,
        pc.product_category_key,
        pc.product_category_name,

        cast(f.trade_value as decimal(38,3)) as trade_value,
        cast(f.quantity as decimal(38,3)) as quantity,

        f.value_unit,
        f.quantity_unit,

        cast(f.trade_value_pre_month as decimal(38,3)) as trade_value_pre_month,
        cast(f.trade_value_pre_year as decimal(38,3)) as trade_value_pre_year,

        cast(f.quantity_pre_month as decimal(38,3)) as quantity_pre_month,
        cast(f.quantity_pre_year as decimal(38,3)) as quantity_pre_year,

        f.created_at,
        f.ingest_at

    from gold_gold.fact_international_trade f

    left join gold_gold.dim_time t
        on f.time_key = t.time_key

    left join gold_gold.dim_product p
        on f.product_key = p.product_key

    left join gold_gold.dim_product_category pc
        on p.product_category_key = pc.product_category_key

),

with_metrics as (

    select
        fact_trade_key,

        time_key,
        full_date,
        report_year,
        report_quarter,
        report_month,

        product_key,
        product_name,
        product_category_key,
        product_category_name,

        trade_value,
        quantity,

        value_unit,
        quantity_unit,

        trade_value_pre_month,
        trade_value_pre_year,

        quantity_pre_month,
        quantity_pre_year,

        cast(
            round(
                trade_value - trade_value_pre_month,
                3
            ) as decimal(38,3)
        ) as trade_value_mom_change,

        cast(
            round(
                
    trade_value - trade_value_pre_month / nullif(trade_value_pre_month, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as mom_growth_rate,

        cast(
            round(
                trade_value - trade_value_pre_year,
                3
            ) as decimal(38,3)
        ) as trade_value_yoy_change,

        cast(
            round(
                
    trade_value - trade_value_pre_year / nullif(trade_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as yoy_growth_rate,

        cast(
            round(
                quantity - quantity_pre_month,
                3
            ) as decimal(38,3)
        ) as quantity_mom_change,

        cast(
            round(
                
    quantity - quantity_pre_month / nullif(quantity_pre_month, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as quantity_mom_growth_rate,

        cast(
            round(
                quantity - quantity_pre_year,
                3
            ) as decimal(38,3)
        ) as quantity_yoy_change,

        cast(
            round(
                
    quantity - quantity_pre_year / nullif(quantity_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as quantity_yoy_growth_rate,

        created_at,
        ingest_at

    from base

),

with_totals as (

    select
        *,

        sum(trade_value) over (
            partition by time_key, value_unit
        ) as total_trade_value,

        sum(trade_value) over (
            partition by time_key, product_category_key, value_unit
        ) as category_total_trade_value

    from with_metrics

),

final as (

    select
        fact_trade_key as mart_trade_international_key,

        time_key,
        full_date,
        report_year,
        report_quarter,
        report_month,

        product_key,
        product_name,
        product_category_key,
        product_category_name,

        trade_value,
        quantity,

        value_unit,
        quantity_unit,

        trade_value_pre_month,
        trade_value_pre_year,

        quantity_pre_month,
        quantity_pre_year,

        trade_value_mom_change,
        mom_growth_rate,

        trade_value_yoy_change,
        yoy_growth_rate,

        quantity_mom_change,
        quantity_mom_growth_rate,

        quantity_yoy_change,
        quantity_yoy_growth_rate,

        cast(
            round(
                
    trade_value / nullif(total_trade_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as product_share_pct,

        cast(
            round(
                
    trade_value / nullif(category_total_trade_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as product_share_in_category_pct,

        created_at,
        ingest_at

    from with_totals

)

select *
from final