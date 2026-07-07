

with indicators as (
    select distinct
        upper(trim(indicator_name)) as indicator_name,        
        upper(trim(indicator_group_name)) as indicator_group_name 
    from gold_staging.stg_macro_indicator
    where indicator_name is not null
)

select
    row_number() over (order by i.indicator_name, i.indicator_group_name) as indicator_key,
    i.indicator_name,
    g.indicator_group_key
from indicators i
left join gold_gold.dim_indicator_group g
    on i.indicator_group_name = g.indicator_group_name