{{
    config(
        materialized='view'
    )
}}

with annual as (

    select
        cast(concat(cast(`year` as string), '-01-01') as date) as report_date,
        cast(`year` as int) as report_year,

        cast(crop_name as string) as crop_name,
        cast('ANNUAL' as string) as crop_group_name,

        cast(production as decimal(38,10)) as production,
        cast(production_unit as string) as production_unit_name,

        cast(area as decimal(38,10)) as area,
        cast(area_unit as string) as area_unit_name,

        cast(`yield` as decimal(38,10)) as source_yield,
        cast(yield_unit as string) as yield_unit_name,

        cast('YEARLY' as string) as period_grain,
        ingest_at

    from {{ source('silver', 'annual_crops') }}

),

staple as (

    select
        cast(concat(cast(`year` as string), '-01-01') as date) as report_date,
        cast(`year` as int) as report_year,

        cast(crop_name as string) as crop_name,
        cast('STAPLE' as string) as crop_group_name,

        cast(production as decimal(38,10)) as production,
        cast(production_unit as string) as production_unit_name,

        cast(area as decimal(38,10)) as area,
        cast(area_unit as string) as area_unit_name,

        cast(`yield` as decimal(38,10)) as source_yield,
        cast(yield_unit as string) as yield_unit_name,

        cast('YEARLY' as string) as period_grain,
        ingest_at

    from {{ source('silver', 'staple_crops') }}

),

perennial as (

    select
        cast(concat(cast(`year` as string), '-01-01') as date) as report_date,
        cast(`year` as int) as report_year,

        cast(crop_name as string) as crop_name,
        cast('PERENNIAL' as string) as crop_group_name,

        cast(production as decimal(38,10)) as production,
        cast(production_unit as string) as production_unit_name,

        cast(area as decimal(38,10)) as area,
        cast(area_unit as string) as area_unit_name,

        cast(`yield` as decimal(38,10)) as source_yield,
        cast(yield_unit as string) as yield_unit_name,

        cast('YEARLY' as string) as period_grain,
        ingest_at

    from {{ source('silver', 'perennial_crops') }}

)

select * from annual
union all
select * from staple
union all
select * from perennial