{{ config(
    materialized='delta_table'
)}}

select
    row_number() over (order by indicator_group_name) as indicator_group_key,
    upper(trim(indicator_group_name)) as indicator_group_name 
from (
    select distinct indicator_group_name
    from {{ ref('stg_macro_indicator') }}
    where indicator_group_name is not null
)