
        CREATE TABLE gold_gold.fact_total_investment
        USING DELTA
        AS
        

with base as (

    select
        cast(to_date(report_date) as date) as report_date,
        capital_source_name,
        unit_name,
        source_name,
        cast(investment_value as decimal(38,10)) as investment_value,
        period_grain,
        ingest_at
    from gold_staging.stg_total_investment

),

joined as (

    select
        cast(dt.time_key as int) as time_key,

        cs.capital_source_key,
        u.unit_key,
        coalesce(s.source_key, cast(-1 as bigint)) as source_key,

        b.investment_value,
        b.period_grain,
        b.ingest_at

    from base b

    left join gold_gold.dim_time dt
        on b.report_date = dt.full_date

    left join gold_gold.dim_capital_source cs
        on b.capital_source_name = cs.source_name

    left join gold_gold.dim_unit u
        on b.unit_name = u.unit_name

    left join gold_gold.dim_source s
        on b.source_name = s.source_name

    where dt.time_key is not null
      and cs.capital_source_key is not null

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(capital_source_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_total_investment_key,

        time_key,
        capital_source_key,
        unit_key,
        source_key,

        cast(round(investment_value, 3) as decimal(38,3)) as investment_value,

        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(capital_source_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
        current_timestamp() as created_at,
        ingest_at

    from joined

)

select *
from final
    