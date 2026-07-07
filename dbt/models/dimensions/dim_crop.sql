{{ config(
    materialized='delta_table'
) }}

with crops as (
    select distinct 
        trim(crop_name) as crop_name, 
        trim(crop_group_name) as crop_group_name
    from {{ ref('stg_crop') }}
    where crop_name is not null
)

select
    row_number() over (order by crop_name) as crop_key,
    crop_name,
    crop_group_name as crop_category
from crops