
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

base as (

    select
        to_date(cast(o.`date` as string)) as report_date,

        o.symbol,
        o.asset_class_name,
        o.market_name,
        o.source_name,

        cast(o.open_price as decimal(38,10)) as open_price,
        cast(o.high_price as decimal(38,10)) as high_price,
        cast(o.low_price as decimal(38,10)) as low_price,
        cast(o.close_price as decimal(38,10)) as close_price,
        cast(o.previous_close as decimal(38,10)) as previous_close,
        cast(o.volume as decimal(38,10)) as volume

    from gold_staging.stg_ohlc o

    where o.`date` is not null

),

joined as (

    select
        cast(t.time_key as int) as time_key,

        a.asset_key,
        coalesce(s.source_key, cast(-1 as bigint)) as source_key,

        b.open_price,
        b.high_price,
        b.low_price,
        b.close_price,
        b.previous_close,
        b.volume

    from base b

    left join gold_gold.dim_time t
        on b.report_date = t.full_date

    left join asset_enriched a
        on b.symbol = a.symbol
       and b.asset_class_name = a.asset_class_name
       and b.market_name = a.market_name

    left join gold_gold.dim_source s
        on b.source_name = s.source_name

    where t.time_key is not null
      and a.asset_key is not null

),

final as (

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

)

select *
from final
    