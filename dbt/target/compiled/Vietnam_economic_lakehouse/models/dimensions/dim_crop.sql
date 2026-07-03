


with crops as (
    select distinct crop_name, crop_group_name
    from gold_staging.stg_agriculture
    where crop_name is not null
)

select
    
    abs(xxhash64(coalesce(cast(c.crop_name as string), '__null__'), coalesce(cast(c.crop_group_name as string), '__null__')))
 as crop_key,
    c.crop_name,
    g.crop_group_key
from crops c
left join gold_gold.dim_crop_group g
    on c.crop_group_name = g.crop_group_name