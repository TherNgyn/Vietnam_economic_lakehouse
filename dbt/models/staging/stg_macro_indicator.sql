{{ config(materialized='view') }}

with m2 as (
    select
        cast(`date` as string) as date_str,
        'M2' as indicator_name,
        'MONEY_SUPPLY' as indicator_group_name,
        cast(m2 as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'm2') }}
),

core_inflation as (
    select
        cast(`date` as string) as date_str,
        'CORE_INFLATION_RATE' as indicator_name,
        'INFLATION' as indicator_group_name,
        cast(core_inflation_rate as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'core_inflation_rate') }}
),

ppi_qoq as (
    select
        cast(`date` as string) as date_str,
        'PPI_QOQ' as indicator_name,
        'PRICE_INDEX' as indicator_group_name,
        cast(ppi_qoq as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'QUARTERLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'ppi_qoq') }}
),

broad_money as (
    select
        cast(`date` as string) as date_str,
        coalesce(indicator, 'BROAD_MONEY') as indicator_name,
        'MONEY_SUPPLY' as indicator_group_name,
        cast(value as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'broad_money') }}
),

cpi_mom_cpi as (
    select
        cast(`date` as string) as date_str,
        'CPI_MOM' as indicator_name,
        'CPI' as indicator_group_name,
        cast(cpi_mom as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'cpi_mom') }}
),

cpi_mom_inflation as (
    select
        cast(`date` as string) as date_str,
        'INFLATION' as indicator_name,
        'INFLATION' as indicator_group_name,
        cast(inflation as decimal(38,10)) as value,
        unit_inflation as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'cpi_mom') }}
),

cpi_base_prev_year as (
    select
        cast(`date` as string) as date_str,
        concat('CPI_BASE_', cpi_base_year, '_PREV_YEAR') as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(prev_year_base as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'cpi_base_year') }}
),

cpi_base_2000 as (
    select
        cast(`date` as string) as date_str,
        'CPI_BASE_2000' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(base_2000 as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'cpi_base_year') }}
),

cpi_base_2005 as (
    select
        cast(`date` as string) as date_str,
        'CPI_BASE_2005' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(base_2005 as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'cpi_base_year') }}
),

cpi_base_2010 as (
    select
        cast(`date` as string) as date_str,
        'CPI_BASE_2010' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(base_2010 as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'cpi_base_year') }}
),

forestry as (
    select
        cast(concat(cast(`year` as string), '-', lpad(cast(case `quarter` when 1 then 1 when 2 then 4 when 3 then 7 when 4 then 10 else 1 end as string), 2, '0'), '-01') as string) as report_date,
        forestry_indicator as indicator_name,
        'FORESTRY' as indicator_group_name,
        cast(value as decimal(38,10)) as value,
        unit as unit_name,
        'GSO_EXCEL' as source_name,
        'QUARTERLY' as period_grain,
        ingest_at
    from {{ source('silver', 'forestry') }}
),

livestock as (
    select
        cast(concat(cast(`year` as string), '-', lpad(cast(case `quarter` when 1 then 1 when 2 then 4 when 3 then 7 when 4 then 10 else 1 end as string), 2, '0'), '-01') as string) as report_date,
        livestock_indicator as indicator_name,
        'LIVESTOCK' as indicator_group_name,
        cast(value as decimal(38,10)) as value,
        unit as unit_name,
        'GSO_EXCEL' as source_name,
        'QUARTERLY' as period_grain,
        ingest_at
    from {{ source('silver', 'livestock') }}
)

select * from m2
union all select * from core_inflation
union all select * from ppi_qoq
union all select * from broad_money
union all select * from cpi_mom_cpi
union all select * from cpi_mom_inflation
union all select * from cpi_base_prev_year
union all select * from cpi_base_2000
union all select * from cpi_base_2005
union all select * from cpi_base_2010
union all select * from forestry
union all select * from livestock