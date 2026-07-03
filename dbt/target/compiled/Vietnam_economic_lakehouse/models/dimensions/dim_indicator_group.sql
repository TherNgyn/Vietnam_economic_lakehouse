


select
    
    abs(xxhash64(coalesce(cast(indicator_group_name as string), '__null__')))
 as indicator_group_key,
    indicator_group_name
from (
    select distinct indicator_group_name
    from gold_staging.stg_macro_indicator
    where indicator_group_name is not null
)