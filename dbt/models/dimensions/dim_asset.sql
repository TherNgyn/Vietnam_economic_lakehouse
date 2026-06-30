{{ config(materialized='table') }}

with assets as (
    select distinct
        symbol,
        asset_name,
        asset_class_name,
        market_name,
        country
    from {{ ref('stg_ohlc') }}

    union

    select distinct
        symbol,
        asset_name,
        asset_class_name,
        market_name,
        country
    from {{ ref('stg_interest_rate') }}
)

select
    {{ sk(['a.symbol', 'a.asset_class_name', 'a.market_name']) }} as asset_key,
    a.symbol,
    a.asset_name,
    ac.asset_class_key,
    m.market_key
from assets a
left join {{ ref('dim_asset_class') }} ac
    on a.asset_class_name = ac.asset_class_name
left join {{ ref('dim_market') }} m
    on a.market_name = m.market_name
   and coalesce(a.country, '__null__') = coalesce(m.country, '__null__')
where a.symbol is not null