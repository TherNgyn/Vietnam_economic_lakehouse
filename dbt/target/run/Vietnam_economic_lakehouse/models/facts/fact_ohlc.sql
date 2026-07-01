
        CREATE TABLE gold_gold.fact_ohlc
        USING DELTA
        AS
        

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
        cast(date_format(to_date(o.date_str), 'yyyyMMdd') as int) as time_key,
        a.asset_key,
        s.source_key,
        o.open_price,
        o.high_price,
        o.low_price,
        o.close_price,
        o.previous_close,
        o.volume
    from gold_staging.stg_ohlc o
    left join asset_enriched a
        on o.symbol = a.symbol
       and o.asset_class_name = a.asset_class_name
       and o.market_name = a.market_name
    left join gold_gold.dim_source s
        on o.source_name = s.source_name
    where o.date_str is not null
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(asset_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_ohlc_key,
    time_key,
    asset_key,
    source_key,
    open_price,
    high_price,
    low_price,
    close_price,
    previous_close,
    volume,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(asset_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
    current_timestamp() as created_at
from joined
where asset_key is not null
    