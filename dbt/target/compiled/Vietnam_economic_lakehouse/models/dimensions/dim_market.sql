


with markets as (
    select distinct market_name, country from gold_staging.stg_ohlc
    union
    select distinct market_name, country from gold_staging.stg_interest_rate
)

select
    
    abs(xxhash64(coalesce(cast(market_name as string), '__null__'), coalesce(cast(country as string), '__null__')))
 as market_key,
    market_name,
    country
from markets
where market_name is not null