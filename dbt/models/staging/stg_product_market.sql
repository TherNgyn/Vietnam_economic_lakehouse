{{ config(materialized='view') }}

with ecommerce as (
    select
        case
            when month is not null then make_date(year, month, 1)
            when quarter = 1 then make_date(year, 1, 1)
            when quarter = 2 then make_date(year, 4, 1)
            when quarter = 3 then make_date(year, 7, 1)
            when quarter = 4 then make_date(year, 10, 1)
            else make_date(year, 1, 1)
        end as report_date,

        product_name,
        type as product_category_name,
        cast(value as decimal(38,10)) as value,
        cast(quantity as decimal(38,10)) as quantity,
        unit as unit_name,
        quantity_unit as quantity_unit_name,
        'GSO_EXCEL' as source_name,

        case
            when month is not null then 'MONTHLY'
            when quarter is not null then 'QUARTERLY'
            else 'YEARLY'
        end as period_grain,

        ingest_at
    from {{ source('silver', 'international_ecommerce') }}
),

industry as (
    select
        case
            when month is not null then make_date(year, month, 1)
            when quarter = 1 then make_date(year, 1, 1)
            when quarter = 2 then make_date(year, 4, 1)
            when quarter = 3 then make_date(year, 7, 1)
            when quarter = 4 then make_date(year, 10, 1)
            else make_date(year, 1, 1)
        end as report_date,

        product_name,
        'INDUSTRY_PRODUCT' as product_category_name,
        cast(value as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        unit as unit_name,
        null as quantity_unit_name,
        'GSO_EXCEL' as source_name,

        case
            when month is not null then 'MONTHLY'
            when quarter is not null then 'QUARTERLY'
            else 'YEARLY'
        end as period_grain,

        ingest_at
    from {{ source('silver', 'industry_product') }}
),

aquatic as (
    select
        make_date(year, case quarter when 1 then 1 when 2 then 4 when 3 then 7 when 4 then 10 else 1 end, 1) as report_date,
        product_name,
        concat('AQUATIC_', aquatic_type) as product_category_name,
        cast(value as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        unit as unit_name,
        null as quantity_unit_name,
        'GSO_EXCEL' as source_name,
        'QUARTERLY' as period_grain,
        ingest_at
    from {{ source('silver', 'aquatic_products') }}
),

gasoline as (
    select
        to_date(date) as report_date,
        product as product_name,
        concat('GASOLINE_', type) as product_category_name,
        cast(price as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        unit as unit_name,
        null as quantity_unit_name,
        source as source_name,
        'DAILY' as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from {{ source('silver', 'gasoline') }}
)

select * from ecommerce
union all select * from industry
union all select * from aquatic
union all select * from gasoline