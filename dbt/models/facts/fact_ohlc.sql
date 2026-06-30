{{
    config(
        materialized='incremental',
        unique_key='fact_ohlc_key'
    )
}}

with asset_enriched as (
    select
        a.asset_key,
        a.symbol,
        ac.asset_class_name,
        m.market_name
    from {{ ref('dim_asset') }} a
    left join {{ ref('dim_asset_class') }} ac
        on a.asset_class_key = ac.asset_class_key
    left join {{ ref('dim_market') }} m
        on a.market_key = m.market_key
),

joined as (
    select
        cast(date_format(o.report_date, 'yyyyMMdd') as int) as time_key,
        a.asset_key,
        s.source_key,
        o.open_price,
        o.high_price,
        o.low_price,
        o.close_price,
        o.previous_close,
        o.volume
    from {{ ref('stg_ohlc') }} o
    left join asset_enriched a
        on o.symbol = a.symbol
       and o.asset_class_name = a.asset_class_name
       and o.market_name = a.market_name
    left join {{ ref('dim_source') }} s
        on o.source_name = s.source_name
    where o.report_date is not null
)

select
    {{ sk(['time_key', 'asset_key', 'source_key']) }} as fact_ohlc_key,
    time_key,
    asset_key,
    source_key,
    open_price,
    high_price,
    low_price,
    close_price,
    previous_close,
    volume,
    {{ sk(['time_key', 'asset_key', 'source_key']) }} as load_id,
    current_timestamp() as created_at
from joined
where asset_key is not null