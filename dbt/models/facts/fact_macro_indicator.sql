{{
    config(
        materialized='incremental',
        unique_key='fact_key'
    )
}}

with joined as (
    select
        cast(date_format(m.report_date, 'yyyyMMdd') as int) as time_key,
        i.indicator_key,
        u.unit_key,
        s.source_key,
        m.period_grain,
        m.value,
        m.ingest_at
    from {{ ref('stg_macro_indicator') }} m
    left join {{ ref('dim_indicator') }} i
        on m.indicator_name = i.indicator_name
    left join {{ ref('dim_unit') }} u
        on m.unit_name = u.unit_name
    left join {{ ref('dim_source') }} s
        on m.source_name = s.source_name
    where m.report_date is not null
)

select
    {{ sk(['time_key', 'indicator_key', 'unit_key', 'source_key', 'period_grain']) }} as fact_key,
    time_key,
    indicator_key,
    unit_key,
    source_key,
    period_grain,
    value,
    {{ sk(['time_key', 'indicator_key', 'unit_key', 'source_key', 'period_grain']) }} as load_id,
    current_timestamp() as created_at
from joined

{% if is_incremental() %}
where ingest_at > (
    select coalesce(max(created_at), timestamp('1900-01-01'))
    from {{ this }}
)
{% endif %}