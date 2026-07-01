


with indicators as (
    select distinct
        indicator_name,
        indicator_group_name
    from gold_staging.stg_macro_indicator
    where indicator_name is not null
)

select
    
    abs(xxhash64(coalesce(cast(i.indicator_name as string), '__null__'), coalesce(cast(i.indicator_group_name as string), '__null__')))
 as indicator_key,
    i.indicator_name,
    g.indicator_group_key
from indicators i
left join gold_gold.dim_indicator_group g
    on i.indicator_group_name = g.indicator_group_name