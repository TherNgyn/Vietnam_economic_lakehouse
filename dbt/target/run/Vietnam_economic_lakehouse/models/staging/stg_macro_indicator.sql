create or replace view gold_staging.stg_macro_indicator
  
  
  as
    

with core_inflation as (
    select
        cast(`date` as date) as date,
        'CORE_INFLATION_RATE' as indicator_name,
        'INFLATION' as indicator_group_name,
        cast(core_inflation_rate as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.core_inflation_rate
),

ppi_qoq as (
    select
        cast(`date` as date) as date,
        'PPI_QOQ' as indicator_name,
        'PRICE_INDEX' as indicator_group_name,
        cast(ppi_qoq as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'QUARTERLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.ppi_qoq
),

broad_money as (
    select
        cast(`date` as date) as date,
        coalesce(upper(trim(indicator)), 'BROAD_MONEY') as indicator_name,
        'MONEY_SUPPLY' as indicator_group_name,
        cast(value as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.broad_money
),

policy_rate as (
    select
        cast(`date` as date) as date,
        coalesce(upper(trim(indicator)), 'POLICY_RATE') as indicator_name,
        'INTEREST_RATE' as indicator_group_name,
        cast(value as decimal(38,10)) as value,
        unit as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.policy_rate
),

cpi_mom_cpi as (
    select
        cast(`date` as date) as date,
        'CPI_MOM' as indicator_name,
        'CPI' as indicator_group_name,
        cast(cpi_mom as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.cpi_mom
),

cpi_mom_inflation as (
    select
        cast(`date` as date) as date,
        'INFLATION' as indicator_name,
        'INFLATION' as indicator_group_name,
        cast(inflation as decimal(38,10)) as value,
        unit_inflation as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.cpi_mom
),

cpi_base_prev_year as (
    select
        cast(`date` as date) as date,
        'CPI_BASE_PREV_YEAR' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(prev_year_base as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.cpi_base_year
),

cpi_base_2000 as (
    select
        cast(`date` as date) as date,
        'CPI_BASE_2000' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(base_2000 as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.cpi_base_year
),

cpi_base_2005 as (
    select
        cast(`date` as date) as date,
        'CPI_BASE_2005' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(base_2005 as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.cpi_base_year
),

cpi_base_2010 as (
    select
        cast(`date` as date) as date,
        'CPI_BASE_2010' as indicator_name,
        'CPI_BASE_YEAR' as indicator_group_name,
        cast(base_2010 as decimal(38,10)) as value,
        unit_cpi as unit_name,
        source as source_name,
        'MONTHLY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.cpi_base_year
)

select * from core_inflation
union all select * from ppi_qoq
union all select * from broad_money
union all select * from policy_rate
union all select * from cpi_mom_cpi
union all select * from cpi_mom_inflation
union all select * from cpi_base_prev_year
union all select * from cpi_base_2000
union all select * from cpi_base_2005
union all select * from cpi_base_2010
