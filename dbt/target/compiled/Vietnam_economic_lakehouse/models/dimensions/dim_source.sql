

with sources as (
    select distinct source_name from gold_staging.stg_macro_indicator
    union
    select distinct source_name from gold_staging.stg_ohlc
    union
    select distinct source_name from gold_staging.stg_interest_rate
    union
    select distinct source_name from gold_staging.stg_product_market
    union
    select distinct source_name from gold_staging.stg_gdp
    union
    select distinct source_name from gold_staging.stg_investment
    union
    select distinct source_name from gold_staging.stg_social_investment
),

final as (
    select
        
    abs(xxhash64(coalesce(cast(source_name as string), '__null__')))
 as source_key,
        source_name,
        source_name as source_system
    from sources
    where source_name is not null
)

select *
from final