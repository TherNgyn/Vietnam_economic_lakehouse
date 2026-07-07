
        CREATE TABLE gold_gold.dim_sub_sector
        USING DELTA
        AS
        

with sub_sectors as (
    select distinct sub_sector_name, sector_name
    from gold_staging.stg_gdp
    where sub_sector_name is not null
)
select
    row_number() over (order by ss.sub_sector_name) as sub_sector_key,
    ss.sub_sector_name,
    s.sector_key
from sub_sectors ss
left join gold_gold.dim_sector s
    on ss.sector_name = s.sector_name
    