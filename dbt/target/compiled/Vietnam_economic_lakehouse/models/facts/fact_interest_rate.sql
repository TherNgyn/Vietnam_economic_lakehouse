

with asset_enriched as (
    select
        a.asset_key,
        a.symbol,
        ac.asset_class_name,
        m.market_name
    from gold_gold.dim_asset a
    left join gold_gold.dim_asset_class ac
        on a.asset_class_key = ac.asset_class_key
    left join gold_gold.dim_market m
        on a.market_key = m.market_key
),

joined as (
    select
        cast(date_format(r.report_date, 'yyyyMMdd') as int) as time_key,
        a.asset_key,
        t.term_key,
        u.unit_key,
        s.source_key,
        r.interest_rate,
        r.volume
    from gold_staging.stg_interest_rate r
    left join asset_enriched a
        on r.symbol = a.symbol
       and r.asset_class_name = a.asset_class_name
       and r.market_name = a.market_name
    left join gold_gold.dim_term t
        on r.term_name = t.term_name
    left join gold_gold.dim_unit u
        on r.unit_name = u.unit_name
    left join gold_gold.dim_source s
        on r.source_name = s.source_name
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(asset_key as string), '__null__'), coalesce(cast(term_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_interest_key,
    time_key,
    asset_key,
    term_key,
    source_key,
    unit_key,
    interest_rate,
    volume,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(asset_key as string), '__null__'), coalesce(cast(term_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
    current_timestamp() as created_at
from joined
where asset_key is not null