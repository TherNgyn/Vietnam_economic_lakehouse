

with units as (

    select distinct unit_name from gold_staging.stg_macro_indicator
    union
    select distinct unit_name from gold_staging.stg_ohlc
    union
    select distinct unit_name from gold_staging.stg_interest_rate
    union
    select distinct unit_name from gold_staging.stg_production_output
    union
    select distinct quantity_unit_name as unit_name from gold_staging.stg_production_output
    union
    select distinct production_unit_name as unit_name from gold_staging.stg_agriculture
    union
    select distinct area_unit_name as unit_name from gold_staging.stg_agriculture
    union
    select distinct yield_unit_name as unit_name from gold_staging.stg_agriculture
    union
    select distinct unit_name from gold_staging.stg_gdp
    union
    select distinct unit_name from gold_staging.stg_investment_by_sector
    union
    select distinct unit_name from gold_staging.stg_social_total_investment

),

cleaned as (

    select distinct
        trim(unit_name) as unit_name
    from units
    where unit_name is not null
      and trim(unit_name) <> ''

),

typed as (

    select distinct
        unit_name,

        case
            when lower(trim(unit_name)) = 'billion vnd' then 'Tỷ đồng'
            when lower(trim(unit_name)) = 'percent' then '%'
            when trim(unit_name) = '%' then '%'
            when lower(trim(unit_name)) = 'vnd/liter' then 'VND/lít'
            when lower(trim(unit_name)) = 'usd/barrel' then 'USD/thùng'
            when lower(trim(unit_name)) = 'usd/gallon' then 'USD/gallon'
            when lower(trim(unit_name)) = 'usd/ounce' then 'USD/ounce'
            when lower(trim(unit_name)) = 'usd/mmbtu' then 'USD/MMBtu'
            when lower(trim(unit_name)) in ('point', 'points') then 'point'
            else trim(unit_name)
        end as unit_nor

    from cleaned

)

select
    
    abs(xxhash64(coalesce(cast(unit_name as string), '__null__')))
 as unit_key,
    unit_name,
    unit_nor
from typed