

with sectors as (
    select distinct sector_name from gold_staging.stg_gdp
),
typed as (
    select distinct trim(sector_name) as sector_name
    from sectors
    where sector_name is not null and trim(sector_name) <> ''
)
select
    row_number() over (order by sector_name) as sector_key,
    sector_name
from typed