
        CREATE TABLE gold_gold.dim_sector
        USING DELTA
        AS
        

with sectors as (

    select distinct sector_name
    from gold_staging.stg_gdp

    union

    select distinct sector_name
    from gold_staging.stg_investment_by_sector

),

typed as (

    select distinct
        trim(sector_name) as sector_name
    from sectors
    where sector_name is not null
      and trim(sector_name) <> ''

)

select
    
    abs(xxhash64(coalesce(cast(sector_name as string), '__null__')))
 as sector_key,
    sector_name
from typed
    