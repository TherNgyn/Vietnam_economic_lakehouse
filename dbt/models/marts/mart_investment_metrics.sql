{{ config(materialized='table') }}

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        f.sub_sector_key,
        ss.sub_sector_name,
        s.sector_name,
        f.unit_key,
        u.unit_name,
        f.investment_value
    from {{ ref('fact_investment') }} f
    left join {{ ref('dim_time') }} t
        on f.time_key = t.time_key
    left join {{ ref('dim_sub_sector') }} ss
        on f.sub_sector_key = ss.sub_sector_key
    left join {{ ref('dim_sector') }} s
        on ss.sector_key = s.sector_key
    left join {{ ref('dim_unit') }} u
        on f.unit_key = u.unit_key
),

calc as (
    select
        *,
        lag(investment_value) over (
            partition by sub_sector_key, unit_key
            order by time_key
        ) as prev_investment_value,

        sum(investment_value) over (
            partition by time_key, unit_key
        ) as total_investment_value
    from base
)

select
    *,
    investment_value - prev_investment_value as investment_growth_value,
    {{ safe_divide('investment_value - prev_investment_value', 'prev_investment_value') }} * 100 as investment_growth_pct,
    {{ safe_divide('investment_value', 'total_investment_value') }} * 100 as investment_share_pct
from calc