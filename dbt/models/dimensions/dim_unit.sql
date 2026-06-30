{{ config(materialized='table') }}

with units as (
    select distinct unit_name from {{ ref('stg_macro_indicator') }}
    union
    select distinct unit_name from {{ ref('stg_ohlc') }}
    union
    select distinct unit_name from {{ ref('stg_interest_rate') }}
    union
    select distinct unit_name from {{ ref('stg_product_market') }}
    union
    select distinct quantity_unit_name as unit_name from {{ ref('stg_product_market') }}
    union
    select distinct production_unit_name as unit_name from {{ ref('stg_agriculture') }}
    union
    select distinct area_unit_name as unit_name from {{ ref('stg_agriculture') }}
    union
    select distinct yield_unit_name as unit_name from {{ ref('stg_agriculture') }}
    union
    select distinct unit_name from {{ ref('stg_gdp') }}
    union
    select distinct unit_name from {{ ref('stg_investment') }}
    union
    select distinct unit_name from {{ ref('stg_social_investment') }}
),

typed as (
    select
        unit_name,
        case
            when lower(unit_name) like '%vnd%' or lower(unit_name) like '%usd%' then 1
            when unit_name = '%' or lower(unit_name) like '%percent%' or lower(unit_name) like '%phần trăm%' then 2
            when lower(unit_name) like '%index%' or lower(unit_name) like '%điểm%' then 3
            when lower(unit_name) like '%tấn%' or lower(unit_name) like '%ton%' or lower(unit_name) like '%nghìn tấn%' then 4
            when lower(unit_name) like '%ha%' or lower(unit_name) like '%hecta%' then 5
            when lower(unit_name) like '%lít%' or lower(unit_name) like '%đồng/lít%' then 6
            else 99
        end as unit_type_key
    from units
    where unit_name is not null
)

select
    {{ sk(['unit_name']) }} as unit_key,
    unit_name,
    unit_type_key
from typed