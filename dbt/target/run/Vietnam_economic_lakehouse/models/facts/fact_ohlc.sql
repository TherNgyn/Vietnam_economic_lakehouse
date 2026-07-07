
        CREATE TABLE gold_gold.fact_ohlc
        USING DELTA
        AS
        

with asset_enriched as (

    select
        a.asset_key,
        upper(trim(a.symbol)) as symbol,
        upper(trim(ac.asset_class_name)) as asset_class_name,
        upper(trim(m.market_name)) as market_name

    from gold_gold.dim_asset a
    left join gold_gold.dim_asset_class ac
        on a.asset_class_key = ac.asset_class_key
    left join gold_gold.dim_market m
        on a.market_key = m.market_key

),

base as (

    select
        to_date(cast(o.`date` as string)) as report_date,
        upper(trim(o.symbol)) as symbol,
        upper(trim(o.asset_class_name)) as asset_class_name,
        upper(trim(o.market_name)) as market_name,
        upper(trim(o.source_name)) as source_name,
        o.unit_name,

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
        b.unit_name,
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
    where t.time_key is not null
      and a.asset_key is not null

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(asset_key as string), '__null__')))
 as fact_ohlc_key,

        time_key,
        asset_key,
        unit_name, 
        cast(round(open_price, 6) as decimal(38,6)) as open_price,
        cast(round(high_price, 6) as decimal(38,6)) as high_price,
        cast(round(low_price, 6) as decimal(38,6)) as low_price,
        cast(round(close_price, 6) as decimal(38,6)) as close_price,
        cast(round(previous_close, 6) as decimal(38,6)) as previous_close,
        cast(round(volume, 2) as decimal(38,2)) as volume,

        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(asset_key as string), '__null__')))
 as load_id,
        current_timestamp() as created_at

    from joined

)

select *
from final
    