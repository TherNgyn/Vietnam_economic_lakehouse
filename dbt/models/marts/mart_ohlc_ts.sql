{{
    config(
        materialized='delta_table'
    )
}}

with base as (

    select
        f.fact_ohlc_key,

        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,
        cast(t.quarter as int) as report_quarter,
        cast(t.month as int) as report_month,
        cast(t.day as int) as report_day,

        f.asset_key,
        a.symbol,
        a.asset_name,

        ac.asset_class_key,
        ac.asset_class_name,

        m.market_key,
        m.market_name,
        m.country,

        f.source_key,
        s.source_name,
        s.source_system,

        cast(f.open_price as decimal(38,10)) as open_price,
        cast(f.high_price as decimal(38,10)) as high_price,
        cast(f.low_price as decimal(38,10)) as low_price,
        cast(f.close_price as decimal(38,10)) as close_price,

        coalesce(
            cast(f.previous_close as decimal(38,10)),
            lag(cast(f.close_price as decimal(38,10))) over (
                partition by f.asset_key, f.source_key
                order by f.time_key
            )
        ) as previous_close,

        cast(f.volume as decimal(38,10)) as volume,

        f.created_at

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

),

with_returns as (

    select
        fact_ohlc_key,

        time_key,
        full_date,
        report_year,
        report_quarter,
        report_month,
        report_day,

        asset_key,
        symbol,
        asset_name,

        asset_class_key,
        asset_class_name,

        market_key,
        market_name,
        country,

        source_key,
        source_name,
        source_system,

        open_price,
        high_price,
        low_price,
        close_price,
        previous_close,
        volume,

        cast(
            round(
                close_price - previous_close,
                6
            ) as decimal(38,6)
        ) as price_change,

        cast(
            round(
                {{ safe_divide('close_price - previous_close', 'previous_close') }} * 100,
                6
            ) as decimal(38,6)
        ) as price_change_pct,

        cast(
            round(
                case
                    when previous_close is not null
                     and previous_close <> 0
                     and close_price is not null
                     and close_price > 0
                    then log(close_price / previous_close)
                end,
                10
            ) as decimal(38,10)
        ) as log_return,

        cast(
            round(
                close_price - open_price,
                6
            ) as decimal(38,6)
        ) as intraday_change,

        cast(
            round(
                {{ safe_divide('close_price - open_price', 'open_price') }} * 100,
                6
            ) as decimal(38,6)
        ) as intraday_change_pct,

        cast(
            round(
                high_price - low_price,
                6
            ) as decimal(38,6)
        ) as high_low_range,

        cast(
            round(
                {{ safe_divide('high_price - low_price', 'low_price') }} * 100,
                6
            ) as decimal(38,6)
        ) as high_low_range_pct,

        created_at

    from base

),

with_rolling as (

    select
        *,

        cast(
            round(
                avg(close_price) over (
                    partition by asset_key, source_key
                    order by time_key
                    rows between 6 preceding and current row
                ),
                6
            ) as decimal(38,6)
        ) as ma_7,

        cast(
            round(
                avg(close_price) over (
                    partition by asset_key, source_key
                    order by time_key
                    rows between 29 preceding and current row
                ),
                6
            ) as decimal(38,6)
        ) as ma_30,

        cast(
            round(
                avg(volume) over (
                    partition by asset_key, source_key
                    order by time_key
                    rows between 6 preceding and current row
                ),
                6
            ) as decimal(38,6)
        ) as volume_ma_7,

        cast(
            round(
                avg(volume) over (
                    partition by asset_key, source_key
                    order by time_key
                    rows between 29 preceding and current row
                ),
                6
            ) as decimal(38,6)
        ) as volume_ma_30,

        cast(
            round(
                stddev_samp(log_return) over (
                    partition by asset_key, source_key
                    order by time_key
                    rows between 6 preceding and current row
                ),
                10
            ) as decimal(38,10)
        ) as volatility_7,

        cast(
            round(
                stddev_samp(log_return) over (
                    partition by asset_key, source_key
                    order by time_key
                    rows between 29 preceding and current row
                ),
                10
            ) as decimal(38,10)
        ) as volatility_30

    from with_returns

),

final as (

    select
        fact_ohlc_key as mart_ohlc_timeseries_key,

        time_key,
        full_date,
        report_year,
        report_quarter,
        report_month,
        report_day,

        asset_key,
        symbol,
        asset_name,

        asset_class_key,
        asset_class_name,

        market_key,
        market_name,
        country,

        source_key,
        source_name,
        source_system,

        open_price,
        high_price,
        low_price,
        close_price,
        previous_close,
        volume,

        price_change,
        price_change_pct,
        log_return,

        intraday_change,
        intraday_change_pct,

        high_low_range,
        high_low_range_pct,

        ma_7,
        ma_30,

        volume_ma_7,
        volume_ma_30,

        volatility_7,
        volatility_30,

        created_at

    from with_rolling

)

select *
from final