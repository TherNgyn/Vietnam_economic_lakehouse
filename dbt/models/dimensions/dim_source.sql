{{ config(
    materialized='delta_table'
)}}

with sources as (
    select distinct source_name from {{ ref('stg_macro_indicator') }}
    union
    select distinct source_name from {{ ref('stg_ohlc') }}
    union
    select distinct source_name from {{ ref('stg_interest_rate') }}
    union
    select distinct source_name from {{ ref('stg_production_output') }}
    union
    select distinct source_name from {{ ref('stg_gdp') }}
    union
    select distinct source_name from {{ ref('stg_total_investment') }}
),

final as (
    select
        {{ sk(['source_name']) }} as source_key,
        source_name,
        source_name as source_system
    from sources
    where source_name is not null
)

select *
from final