{{ config(materialized='view') }}

select
    cast(`date` as string) as date_str,
    symbol,
    symbol as asset_name,
    'INTEREST_RATE' as asset_class_name,
    'MONEY_MARKET' as market_name,
    null as country,
    term as term_name,
    cast(interest_rate as decimal(38,10)) as interest_rate,
    cast(volume as decimal(38,10)) as volume,
    '%' as unit_name,
    source as source_name,
    cast(processing_date as timestamp) as ingest_at
from {{ source('silver', 'interest_rate') }}