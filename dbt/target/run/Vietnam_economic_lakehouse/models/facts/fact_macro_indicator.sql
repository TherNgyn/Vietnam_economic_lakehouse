
        CREATE TABLE gold_gold.fact_macro_indicator
        USING DELTA
        AS
        

with base as (

    select
        to_date(cast(m.`date` as string)) as report_date,

        m.indicator_name,
        m.unit_name,
        m.source_name,
        m.period_grain,

        cast(m.value as decimal(38,10)) as value,
        m.ingest_at

    from gold_staging.stg_macro_indicator m

    where m.`date` is not null

),

joined as (

    select
        cast(t.time_key as int) as time_key,

        i.indicator_key,
        u.unit_key,
        coalesce(s.source_key, cast(-1 as bigint)) as source_key,

        b.period_grain,
        b.value,
        b.ingest_at

    from base b

    left join gold_gold.dim_time t
        on b.report_date = t.full_date

    left join gold_gold.dim_indicator i
        on b.indicator_name = i.indicator_name

    left join gold_gold.dim_unit u
        on b.unit_name = u.unit_name

    left join gold_gold.dim_source s
        on b.source_name = s.source_name

    where t.time_key is not null
      and i.indicator_key is not null

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(indicator_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__'), coalesce(cast(period_grain as string), '__null__')))
 as fact_key,

        time_key,
        indicator_key,
        unit_key,
        source_key,
        period_grain,

        cast(round(value, 6) as decimal(38,6)) as value,

        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(indicator_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__'), coalesce(cast(period_grain as string), '__null__')))
 as load_id,
        current_timestamp() as created_at,
        ingest_at

    from joined

)

select *
from final
    