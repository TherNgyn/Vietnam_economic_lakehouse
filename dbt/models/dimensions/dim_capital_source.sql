{{ config(materialized='table') }}

select
    {{ sk(['capital_source_name']) }} as capital_source_key,
    capital_source_name as source_name
from (
    select distinct capital_source_name
    from {{ ref('stg_social_investment') }}
    where capital_source_name is not null
)