
        CREATE TABLE gold_gold.dim_asset_class
        USING DELTA
        AS
        



with classes as (
    select distinct asset_class_name from gold_staging.stg_ohlc
    union
    select distinct asset_class_name from gold_staging.stg_interest_rate
)

select
    
    abs(xxhash64(coalesce(cast(asset_class_name as string), '__null__')))
 as asset_class_key,
    asset_class_name
from classes
where asset_class_name is not null
    