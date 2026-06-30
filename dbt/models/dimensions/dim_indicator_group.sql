{{ config(materialized='table') }}

select
    {{ sk(['indicator_group_name']) }} as indicator_group_key,
    indicator_group_name
from (
    select distinct indicator_group_name
    from {{ ref('stg_macro_indicator') }}
    where indicator_group_name is not null
)