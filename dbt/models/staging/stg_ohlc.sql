{{ config(
    materialized='view'
) }}

with currency as (
    select
        cast(`date` as date) as date,
        upper(trim(symbol)) as symbol,
        upper(trim(symbol)) as asset_name,
        upper(trim(asset_class)) as asset_class_name,
        upper(trim(unit)) as unit_name,
        'GLOBAL' as market_name,
        cast(null as string) as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(prev_close as decimal(38,10)) as previous_close,
        upper(trim(source)) as source_name
    from {{ source('silver', 'ohlc_currency') }} -- Tên bảng tương ứng bucket s3://silver/ohlc_currency
),

idx as (
    select
        cast(`date` as date) as date,
        upper(trim(symbol)) as symbol,
        upper(trim(symbol)) as asset_name,
        upper(trim(asset_class)) as asset_class_name,
        upper(trim(unit)) as unit_name,
        'GLOBAL' as market_name,
        cast(null as string) as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(prev_close as decimal(38,10)) as previous_close,
        upper(trim(source)) as source_name
    from {{ source('silver', 'ohlc_index') }} -- Tên bảng tương ứng bucket s3://silver/index
),

commodity as (
    select
        cast(`date` as date) as date,
        upper(trim(symbol)) as symbol,
        upper(trim(symbol)) as asset_name,
        upper(trim(asset_class)) as asset_class_name,
        upper(trim(unit)) as unit_name,
        'GLOBAL' as market_name,
        cast(null as string) as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(prev_close as decimal(38,10)) as previous_close,
        upper(trim(source)) as source_name
    from {{ source('silver', 'ohlc_commodity') }} -- Tên bảng tương ứng bucket s3://silver/commodity
),

vietnam_index as (
    select
        cast(`date` as date) as date,
        upper(trim(symbol)) as symbol,
        upper(trim(symbol)) as asset_name,
        upper(trim(asset_class)) as asset_class_name,
        upper(trim(unit)) as unit_name, -- Đã sửa: lấy chính xác cột unit ('point') từ python
        'VIETNAM' as market_name,
        'VN' as country,
        cast(open as decimal(38,10)) as open_price,
        cast(high as decimal(38,10)) as high_price,
        cast(low as decimal(38,10)) as low_price,
        cast(close as decimal(38,10)) as close_price,
        cast(volume as decimal(38,10)) as volume,
        cast(null as decimal(38,10)) as previous_close,
        upper(trim(source)) as source_name -- Gồm 'historical_csv' hoặc 'yfinance' từ python
    from {{ source('silver', 'ohlc_vietnam_index') }} -- Tên bảng tương ứng bucket s3://silver/vietnam_index
)

select * from currency
union all select * from idx
union all select * from commodity
union all select * from vietnam_index