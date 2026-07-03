

with base as (

    select
        f.fact_total_investment_key,

        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,
        cast(t.quarter as int) as report_quarter,

        f.capital_source_key,
        cs.source_name as capital_source_name,

        f.unit_key,
        u.unit_name,
        u.unit_nor,

        f.source_key,
        s.source_name,
        s.source_system,

        cast(f.investment_value as decimal(38,3)) as investment_value,
        cast(f.investment_value_pre_quarter as decimal(38,3)) as investment_value_pre_quarter,
        cast(f.investment_value_pre_year as decimal(38,3)) as investment_value_pre_year,

        f.period_grain,
        f.created_at,
        f.ingest_at

    from gold_gold.fact_social_total_investment f

    left join gold_gold.dim_time t
        on f.time_key = t.time_key

    left join gold_gold.dim_capital_source cs
        on f.capital_source_key = cs.capital_source_key

    left join gold_gold.dim_unit u
        on f.unit_key = u.unit_key

    left join gold_gold.dim_source s
        on f.source_key = s.source_key

),

with_metrics as (

    select
        fact_total_investment_key,

        time_key,
        full_date,
        report_year,
        report_quarter,

        capital_source_key,
        capital_source_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        investment_value,
        investment_value_pre_quarter,
        investment_value_pre_year,

        cast(
            round(
                
    investment_value - investment_value_pre_quarter / nullif(investment_value_pre_quarter, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as qoq_growth_rate,

        cast(
            round(
                
    investment_value - investment_value_pre_year / nullif(investment_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as yoy_growth_rate,

        period_grain,
        created_at,
        ingest_at

    from base

),

with_total as (

    select
        *,

        sum(investment_value) over (
            partition by time_key, unit_key, source_key
        ) as total_investment_value

    from with_metrics

),

final as (

    select
        fact_total_investment_key as mart_total_investment_key,

        time_key,
        full_date,
        report_year,
        report_quarter,

        capital_source_key,
        capital_source_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        investment_value,
        investment_value_pre_quarter,
        investment_value_pre_year,

        qoq_growth_rate,
        yoy_growth_rate,

        cast(
            round(
                
    investment_value / nullif(total_investment_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as source_share_pct,

        period_grain,
        created_at,
        ingest_at

    from with_total

)

select *
from final