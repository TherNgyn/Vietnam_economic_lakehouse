
        CREATE TABLE gold_gold.dim_asset
        USING DELTA
        AS
        

with assets as (
    select distinct
        upper(trim(symbol)) as symbol,
        upper(trim(asset_name)) as asset_name,
        upper(trim(asset_class_name)) as asset_class_name,
        upper(trim(market_name)) as market_name,
        upper(trim(country)) as country
    from gold_staging.stg_ohlc

    union

    select distinct
        upper(trim(symbol)) as symbol,
        upper(trim(asset_name)) as asset_name,
        upper(trim(asset_class_name)) as asset_class_name,
        upper(trim(market_name)) as market_name,
        upper(trim(country)) as country
    from gold_staging.stg_interest_rate
)

select
    row_number() over (order by a.symbol, a.asset_class_name, a.market_name) as asset_key,
    a.symbol,
    a.asset_name,
    ac.asset_class_key,
    m.market_key
from assets a
left join gold_gold.dim_asset_class ac
    on a.asset_class_name = ac.asset_class_name
left join gold_gold.dim_market m
    on a.market_name = m.market_name
   and coalesce(a.country, '__NULL__') = coalesce(m.country, '__NULL__')
where a.symbol is not null
    