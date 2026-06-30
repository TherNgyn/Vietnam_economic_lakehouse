{{ config(materialized='view') }}

with currency as (
    select
        to_date(date) as report_date,
        symbol,
        symbol as asset_name,
        asset_class as asset_class_name,
        unit as unit_name,
        'GLOBAL' as market_name,
        null as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(prev_close as decimal(38,10)) as previous_close,
        source as source_name
    from {{ source('silver', 'ohlc_currency') }}
),

idx as (
    select
        to_date(date) as report_date,
        symbol,
        symbol as asset_name,
        asset_class as asset_class_name,
        unit as unit_name,
        'GLOBAL' as market_name,
        null as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(prev_close as decimal(38,10)) as previous_close,
        source as source_name
    from {{ source('silver', 'ohlc_index') }}
),

commodity as (
    select
        to_date(date) as report_date,
        symbol,
        symbol as asset_name,
        asset_class as asset_class_name,
        unit as unit_name,
        'GLOBAL' as market_name,
        null as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(prev_close as decimal(38,10)) as previous_close,
        source as source_name
    from {{ source('silver', 'ohlc_commodity') }}
),

vietnam_index as (
    select
        to_date(date) as report_date,
        symbol,
        symbol as asset_name,
        asset_class as asset_class_name,
        unit as unit_name,
        'VIETNAM' as market_name,
        'VN' as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(null as decimal(38,10)) as previous_close,
        source as source_name
    from {{ source('silver', 'ohlc_vietnam_index') }}
)

select * from currency
union all select * from idx
union all select * from commodity
union all select * from vietnam_index