{{
    config(
        materialized='delta_table'
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

    from {{ ref('stg_ohlc') }} o

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

    left join {{ ref('dim_time') }} t
        on b.report_date = t.full_date

    left join asset_enriched a
        on b.symbol = a.symbol
       and b.asset_class_name = a.asset_class_name
       and b.market_name = a.market_name

    left join {{ ref('dim_source') }} s
        on b.source_name = s.source_name

    where t.time_key is not null
      and a.asset_key is not null

),

final as (

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

)

select *
from final