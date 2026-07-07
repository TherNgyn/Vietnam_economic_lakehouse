{{ config(
    materialized='delta_table'
) }}

with fact as (
    select
        f.fact_total_investment_key,
        f.time_key,
        f.capital_source_key,
        f.unit,
        f.investment_value,
        f.investment_value_pre_quarter,
        f.investment_value_pre_year,
        f.period_grain,
        f.created_at,
        f.ingest_at,
        t.year as report_year,
        t.quarter as report_quarter
    from {{ ref('fact_social_total_investment') }} f
    left join {{ ref('dim_time') }} t
        on f.time_key = t.time_key
),

with_growth as (
    select
        *,
        case
            when investment_value_pre_quarter > 0
            then round(
                (
                    investment_value - investment_value_pre_quarter
                ) / investment_value_pre_quarter * 100,
                3
            )
            else 0
        end as qoq_growth_rate,

        case
            when investment_value_pre_year > 0
            then round(
                (
                    investment_value - investment_value_pre_year
                ) / investment_value_pre_year * 100,
                3
            )
            else 0
        end as yoy_growth_rate
    from fact
),

with_share as (
    select
        *,
        case
            when sum(investment_value) over (
                partition by report_year, report_quarter
            ) > 0
            then round(
                investment_value
                / sum(investment_value) over (
                    partition by report_year, report_quarter
                ) * 100,
                3
            )
            else 0
        end as source_share_pct
    from with_growth
)

select
    fact_total_investment_key,
    time_key,
    capital_source_key,
    unit,

    investment_value,
    investment_value_pre_quarter,
    investment_value_pre_year,

    cast(coalesce(qoq_growth_rate, 0) as float) as qoq_growth_rate,
    cast(coalesce(yoy_growth_rate, 0) as float) as yoy_growth_rate,
    cast(coalesce(source_share_pct, 0) as float) as source_share_pct,

    period_grain,
    created_at,
    ingest_at
from with_share