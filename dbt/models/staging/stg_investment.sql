{{ config(materialized='view') }}

select
    make_date(year, 1, 1) as report_date,
    name as sector_name,
    name as sub_sector_name,
    cast(value as decimal(38,10)) as investment_value,
    unit as unit_name,
    'GSO_EXCEL' as source_name,
    'YEARLY' as period_grain,
    ingest_at
from {{ source('silver', 'investment_by_sector') }}