
        CREATE TABLE gold_gold.fact_macro_indicator
        USING DELTA
        AS
        

with joined as (
    select
        cast(date_format(to_date(m.date_str), 'yyyyMMdd') as int) as time_key,
        i.indicator_key,
        u.unit_key,
        s.source_key,
        m.period_grain,
        m.value,
        m.ingest_at
    from gold_staging.stg_macro_indicator m
    left join gold_gold.dim_indicator i
        on m.indicator_name = i.indicator_name
    left join gold_gold.dim_unit u
        on m.unit_name = u.unit_name
    left join gold_gold.dim_source s
        on m.source_name = s.source_name
    where m.date_str is not null
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(indicator_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__'), coalesce(cast(period_grain as string), '__null__')))
 as fact_key,
    time_key,
    indicator_key,
    unit_key,
    source_key,
    period_grain,
    value,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(indicator_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__'), coalesce(cast(period_grain as string), '__null__')))
 as load_id,
    current_timestamp() as created_at
from joined


    