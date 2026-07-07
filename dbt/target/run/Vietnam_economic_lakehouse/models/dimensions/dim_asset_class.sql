
        CREATE TABLE gold_gold.dim_asset_class
        USING DELTA
        AS
        

with classes as (
    select distinct upper(trim(asset_class_name)) as asset_class_name from gold_staging.stg_ohlc
    union
    select distinct upper(trim(asset_class_name)) as asset_class_name from gold_staging.stg_interest_rate
)

select
    row_number() over (order by asset_class_name) as asset_class_key,
    asset_class_name
from classes
where asset_class_name is not null
    