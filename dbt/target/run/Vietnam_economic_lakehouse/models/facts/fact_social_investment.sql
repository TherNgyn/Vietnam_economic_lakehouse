
        CREATE TABLE gold_gold.fact_social_investment
        USING DELTA
        AS
        

with joined as (
    select
        cast(date_format(to_date(i.report_date), 'yyyyMMdd') as int) as time_key,
        cs.capital_source_key,
        u.unit_key,
        s.source_key,
        i.investment_value
    from gold_staging.stg_social_investment i
    left join gold_gold.dim_capital_source cs
        on i.capital_source_name = cs.source_name
    left join gold_gold.dim_unit u
        on i.unit_name = u.unit_name
    left join gold_gold.dim_source s
        on i.source_name = s.source_name
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(capital_source_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_social_key,
    time_key,
    capital_source_key,
    unit_key,
    source_key,
    investment_value,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(capital_source_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
    current_timestamp() as created_at
from joined
where capital_source_key is not null
    