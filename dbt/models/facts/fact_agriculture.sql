{{
    config(
        materialized='incremental',
        unique_key='fact_agri_key'
    )
}}

with joined as (
    select
        cast(date_format(a.report_date, 'yyyyMMdd') as int) as time_key,
        c.crop_key,
        cast(null as bigint) as source_key,
        a.production,
        a.area
    from {{ ref('stg_agriculture') }} a
    left join {{ ref('dim_crop') }} c
        on a.crop_name = c.crop_name
)

select
    {{ sk(['time_key', 'crop_key', 'source_key']) }} as fact_agri_key,
    time_key,
    crop_key,
    source_key,
    production,
    area,
    {{ sk(['time_key', 'crop_key', 'source_key']) }} as load_id,
    current_timestamp() as created_at
from joined
where crop_key is not null