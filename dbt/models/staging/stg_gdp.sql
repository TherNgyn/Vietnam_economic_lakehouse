{{ config(materialized='view') }}

select
    make_date(year, case quarter when 1 then 1 when 2 then 4 when 3 then 7 when 4 then 10 else 1 end, 1) as report_date,
    sector as sector_name,
    sub_sector as sub_sector_name,
    type as gdp_type,
    cast(value as decimal(38,10)) as value,
    unit as unit_name,
    'GSO_EXCEL' as source_name,
    'QUARTERLY' as period_grain,
    ingest_at
from {{ source('silver', 'gdp') }}