{{ config(materialized='table') }}

with base as (
    select
        f.time_key,
        t.full_date,
        t.year,
        t.month,
        t.quarter,

        f.asset_key,
        a.symbol,
        a.asset_name,
        ac.asset_class_name,
        m.market_name,

        f.source_key,
        s.source_name,

        f.open_price,
        f.high_price,
        f.low_price,
        f.close_price,

        coalesce(
            f.previous_close,
            lag(f.close_price) over (
                partition by f.asset_key, f.source_key
                order by f.time_key
            )
        ) as previous_close,

        f.volume
    from {{ ref('fact_ohlc') }} f
    left join {{ ref('dim_time') }} t
        on f.time_key = t.time_key
    left join {{ ref('dim_asset') }} a
        on f.asset_key = a.asset_key
    left join {{ ref('dim_asset_class') }} ac
        on a.asset_class_key = ac.asset_class_key
    left join {{ ref('dim_market') }} m
        on a.market_key = m.market_key
    left join {{ ref('dim_source') }} s
        on f.source_key = s.source_key
)

select
    *,
    close_price - previous_close as price_change,
    {{ safe_divide('close_price - previous_close', 'previous_close') }} * 100 as price_change_pct,

    case
        when previous_close is not null and previous_close <> 0
        then log(close_price / previous_close)
    end as log_return,

    avg(close_price) over (
        partition by asset_key
        order by time_key
        rows between 6 preceding and current row
    ) as ma_7,

    avg(close_price) over (
        partition by asset_key
        order by time_key
        rows between 29 preceding and current row
    ) as ma_30

from base