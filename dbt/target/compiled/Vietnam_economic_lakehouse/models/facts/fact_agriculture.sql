

with joined as (
    select
        cast(date_format(a.report_date, 'yyyyMMdd') as int) as time_key,
        c.crop_key,
        cast(null as bigint) as source_key,
        a.production,
        a.area
    from gold_staging.stg_agriculture a
    left join gold_gold.dim_crop c
        on a.crop_name = c.crop_name
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(crop_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_agri_key,
    time_key,
    crop_key,
    source_key,
    production,
    area,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(crop_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
    current_timestamp() as created_at
from joined
where crop_key is not null