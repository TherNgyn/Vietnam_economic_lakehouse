


with sub_sectors as (
    select distinct sub_sector_name
    from gold_staging.stg_gdp
    where sub_sector_name is not null
)

select
    
    abs(xxhash64(coalesce(cast(sub_sector_name as string), '__null__')))
 as sub_sector_key,
    sub_sector_name
from sub_sectors