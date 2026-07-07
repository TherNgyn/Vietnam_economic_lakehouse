{{ config(
    materialized='delta_table'
) }}

with base as (

    select
        to_date(cast(m.`date` as string)) as report_date,
        m.indicator_name,
        m.unit_name,
        m.source_name,
        m.period_grain,
        cast(m.value as decimal(38,10)) as value,
        m.ingest_at
    from {{ ref('stg_macro_indicator') }} m
    where m.`date` is not null

),

joined as (

    select
        cast(t.time_key as int) as time_key,
        i.indicator_key,
        b.unit_name,  
        b.period_grain,
        b.value,
        b.ingest_at
    from base b
    left join {{ ref('dim_time') }} t
        on b.report_date = t.full_date
    left join {{ ref('dim_indicator') }} i
        on b.indicator_name = i.indicator_name
    where t.time_key is not null
      and i.indicator_key is not null

),

final as (

    select
        {{ sk(['time_key', 'indicator_key', 'period_grain']) }} as fact_key,
        time_key,
        indicator_key,
        unit_name,   
        period_grain,
        cast(round(value, 6) as decimal(38,6)) as value,
        {{ sk(['time_key', 'indicator_key', 'period_grain']) }} as load_id,
        current_timestamp() as created_at,
        ingest_at
    from joined

)

select *
from final