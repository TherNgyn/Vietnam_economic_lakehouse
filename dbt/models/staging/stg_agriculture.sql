{{ config(materialized='view') }}

with annual as (
    select
        make_date(year, 1, 1) as report_date,
        crop_name,
        'ANNUAL' as crop_group_name,
        cast(production as decimal(38,10)) as production,
        production_unit as production_unit_name,
        cast(area as decimal(38,10)) as area,
        area_unit as area_unit_name,
        cast(yield as decimal(38,10)) as source_yield,
        yield_unit as yield_unit_name,
        'YEARLY' as period_grain,
        ingest_at
    from {{ source('silver', 'annual_crops') }}
),

staple as (
    select
        make_date(year, 1, 1) as report_date,
        crop_name,
        'STAPLE' as crop_group_name,
        cast(production as decimal(38,10)) as production,
        production_unit as production_unit_name,
        cast(area as decimal(38,10)) as area,
        area_unit as area_unit_name,
        cast(yield as decimal(38,10)) as source_yield,
        yield_unit as yield_unit_name,
        'YEARLY' as period_grain,
        ingest_at
    from {{ source('silver', 'staple_crops') }}
),

perennial as (
    select
        make_date(year, 1, 1) as report_date,
        crop_name,
        'PERENNIAL' as crop_group_name,
        cast(production as decimal(38,10)) as production,
        production_unit as production_unit_name,
        cast(area as decimal(38,10)) as area,
        area_unit as area_unit_name,
        cast(yield as decimal(38,10)) as source_yield,
        yield_unit as yield_unit_name,
        'YEARLY' as period_grain,
        ingest_at
    from {{ source('silver', 'perennial_crops') }}
)

select * from annual
union all select * from staple
union all select * from perennial