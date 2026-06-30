{{
    config(
        materialized='incremental',
        unique_key='fact_social_key'
    )
}}

with joined as (
    select
        cast(date_format(i.report_date, 'yyyyMMdd') as int) as time_key,
        cs.capital_source_key,
        u.unit_key,
        s.source_key,
        i.investment_value
    from {{ ref('stg_social_investment') }} i
    left join {{ ref('dim_capital_source') }} cs
        on i.capital_source_name = cs.source_name
    left join {{ ref('dim_unit') }} u
        on i.unit_name = u.unit_name
    left join {{ ref('dim_source') }} s
        on i.source_name = s.source_name
)

select
    {{ sk(['time_key', 'capital_source_key', 'unit_key', 'source_key']) }} as fact_social_key,
    time_key,
    capital_source_key,
    unit_key,
    source_key,
    investment_value,
    {{ sk(['time_key', 'capital_source_key', 'unit_key', 'source_key']) }} as load_id,
    current_timestamp() as created_at
from joined
where capital_source_key is not null