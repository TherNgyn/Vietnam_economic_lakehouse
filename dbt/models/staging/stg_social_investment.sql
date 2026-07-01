{{ config(materialized='view') }}

select
    cast(concat(cast(`year` as string), '-', lpad(cast(case `quarter` when 1 then 1 when 2 then 4 when 3 then 7 when 4 then 10 else 1 end as string), 2, '0'), '-01') as string) as report_date,
    investment_name as capital_source_name,
    cast(value as decimal(38,10)) as investment_value,
    unit as unit_name,
    'GSO_EXCEL' as source_name,
    'QUARTERLY' as period_grain,
    ingest_at
from {{ source('silver', 'investment') }}