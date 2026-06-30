{{ config(materialized='table') }}

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        t.month,
        t.quarter,
        f.indicator_key,
        i.indicator_name,
        ig.indicator_group_name,
        f.unit_key,
        u.unit_name,
        f.source_key,
        s.source_name,
        f.period_grain,
        f.value
    from {{ ref('fact_macro_indicator') }} f
    left join {{ ref('dim_time') }} t
        on f.time_key = t.time_key
    left join {{ ref('dim_indicator') }} i
        on f.indicator_key = i.indicator_key
    left join {{ ref('dim_indicator_group') }} ig
        on i.indicator_group_key = ig.indicator_group_key
    left join {{ ref('dim_unit') }} u
        on f.unit_key = u.unit_key
    left join {{ ref('dim_source') }} s
        on f.source_key = s.source_key
),

calc as (
    select
        *,
        lag(value, 1) over (
            partition by indicator_key, unit_key, source_key, period_grain
            order by time_key
        ) as prev_value,

        lag(value, 4) over (
            partition by indicator_key, unit_key, source_key, period_grain
            order by time_key
        ) as prev_4_period_value,

        lag(value, 12) over (
            partition by indicator_key, unit_key, source_key, period_grain
            order by time_key
        ) as same_period_last_year_value
    from base
)

select
    *,
    value - prev_value as change_value,
    {{ safe_divide('value - prev_value', 'prev_value') }} * 100 as growth_pct,

    value - same_period_last_year_value as yoy_change_value,
    {{ safe_divide('value - same_period_last_year_value', 'same_period_last_year_value') }} * 100 as yoy_growth_pct,

    value - prev_4_period_value as qoq_change_value,
    {{ safe_divide('value - prev_4_period_value', 'prev_4_period_value') }} * 100 as qoq_growth_pct

from calc