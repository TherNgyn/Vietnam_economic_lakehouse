{{
    config(
        materialized='delta_table'
    )
}}

with sources as (

    select distinct source_name
    from {{ ref('stg_macro_indicator') }}

    union

    select distinct source_name
    from {{ ref('stg_ohlc') }}

    union

    select distinct source_name
    from {{ ref('stg_interest_rate') }}

    union

    select distinct source_name
    from {{ ref('stg_production_output') }}

    -- union

    -- select distinct source_name
    -- from {{ ref('stg_agriculture') }}

    union

    select distinct source_name
    from {{ ref('stg_gdp') }}

    union

    select distinct source_name
    from {{ ref('stg_investment_by_sector') }}

    union

    select distinct source_name
    from {{ ref('stg_social_total_investment') }}

),

cleaned as (

    select distinct
        trim(source_name) as source_name
    from sources
    where source_name is not null
      and trim(source_name) <> ''

),

typed as (

    select distinct
        source_name,

        case
            when lower(trim(source_name)) = 'realtime'
                then 'Vikkibanks'

            when lower(trim(source_name)) = 'historical_csv'
                then 'Investing.com'

            when lower(trim(source_name)) = 'yfinance'
                then 'Yahoo Finance'

            when lower(trim(source_name)) = 'scraped'
                then 'Vikkibanks/Yahoo Finance'

            when lower(trim(source_name)) = 'gso_excel'
                then 'General Statistics Office of Vietnam'

            when lower(trim(source_name)) = 'gso'
                then 'General Statistics Office of Vietnam'

            else trim(source_name)
        end as source_system

    from cleaned

)

select
    {{ sk(['source_name']) }} as source_key,
    source_name,
    source_system
from typed