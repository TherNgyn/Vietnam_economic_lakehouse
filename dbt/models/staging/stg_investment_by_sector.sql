{{ config(materialized='view') }}

select
    cast(concat(cast(`year` as string), '-01-01') as date) as report_date,
    name as sector_name,
    cast(value as decimal(38,10)) as value,
    unit as unit_name,
    'GSO_EXCEL' as source_name,
    'YEARLY' as period_grain,
    ingest_at
from {{ source('silver', 'investment_by_sector') }}