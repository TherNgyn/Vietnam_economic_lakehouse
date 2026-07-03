create or replace view gold_staging.stg_interest_rate
  
  
  as
    

select
    cast(`date` as date) as date,
    symbol,
    symbol as asset_name,
    'INTEREST_RATE' as asset_class_name,
    'MONEY_MARKET' as market_name,
    'VN' as country,
    term as term_name,
    cast(interest_rate as decimal(38,10)) as interest_rate,
    cast(volume as decimal(38,10)) as volume,
    '%' as unit_name,
    source as source_name,
    cast(processing_date as timestamp) as ingest_at
from silver.interest_rate
