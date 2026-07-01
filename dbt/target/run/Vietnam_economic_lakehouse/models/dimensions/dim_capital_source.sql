
        CREATE TABLE gold_gold.dim_capital_source
        USING DELTA
        AS
        


select
    
    abs(xxhash64(coalesce(cast(capital_source_name as string), '__null__')))
 as capital_source_key,
    capital_source_name as source_name
from (
    select distinct capital_source_name
    from gold_staging.stg_social_investment
    where capital_source_name is not null
)
    