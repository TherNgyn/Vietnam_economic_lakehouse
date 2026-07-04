
        CREATE TABLE gold_gold.dim_source
        USING DELTA
        AS
        

with sources as (

    select distinct source_name
    from gold_staging.stg_macro_indicator

    union

    select distinct source_name
    from gold_staging.stg_ohlc

    union

    select distinct source_name
    from gold_staging.stg_interest_rate

    union

    select distinct source_name
    from gold_staging.stg_production_output

    -- union

    -- select distinct source_name
    -- from gold_staging.stg_agriculture

    union

    select distinct source_name
    from gold_staging.stg_gdp

    union

    select distinct source_name
    from gold_staging.stg_investment_by_sector

    union

    select distinct source_name
    from gold_staging.stg_social_total_investment

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
    
    abs(xxhash64(coalesce(cast(source_name as string), '__null__')))
 as source_key,
    source_name,
    source_system
from typed
    