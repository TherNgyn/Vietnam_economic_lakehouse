

with industry as (
    select
        cast(
            concat(
                cast(year as string),
                '-',
                lpad(cast(
                    case
                        when month is not null then month
                        when quarter = 1 then 1
                        when quarter = 2 then 4
                        when quarter = 3 then 7
                        when quarter = 4 then 10
                        else 1
                    end as string
                ), 2, '0'),
                '-01'
            ) as date
        ) as report_date,
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
    from silver.industry_product
),

livestock as (
    select
        cast(
            concat(
                cast(`year` as string),
                '-',
                lpad(
                    cast(
                        case cast(`quarter` as int)
                            when 1 then 1
                            when 2 then 4
                            when 3 then 7
                            when 4 then 10
                            else 1
                        end as string
                    ),
                    2,
                    '0'
                ),
                '-01'
            ) as date
        ) as report_date,
        trim(cast(livestock_indicator as string)) as product_name,
        cast('LIVESTOCK_PRODUCT' as string) as product_category_name,
        cast(value as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        trim(cast(unit as string)) as unit_name,
        cast(null as string) as quantity_unit_name,
        cast('GSO_EXCEL' as string) as source_name,
        cast('QUARTERLY' as string) as period_grain,
        cast(ingest_at as timestamp) as ingest_at
    from silver.livestock

    where `year` is not null
      and `quarter` between 1 and 4
      and livestock_indicator is not null

),

forestry as (

    select
        cast(
            concat(
                cast(`year` as string),
                '-',
                lpad(
                    cast(
                        case cast(`quarter` as int)
                            when 1 then 1
                            when 2 then 4
                            when 3 then 7
                            when 4 then 10
                            else 1
                        end as string
                    ),
                    2,
                    '0'
                ),
                '-01'
            ) as date
        ) as report_date,
        trim(cast(forestry_indicator as string)) as product_name,
        cast('FORESTRY_PRODUCT' as string) as product_category_name,
        cast(value as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        trim(cast(unit as string)) as unit_name,
        cast(null as string) as quantity_unit_name,
        cast('GSO_EXCEL' as string) as source_name,
        cast('QUARTERLY' as string) as period_grain,
        cast(ingest_at as timestamp) as ingest_at
    from silver.forestry
    where `year` is not null
      and `quarter` between 1 and 4
      and forestry_indicator is not null

),

aquatic as (
    select
        cast(
            concat(
                cast(year as string),
                '-',
                lpad(cast(
                    case quarter
                        when 1 then 1
                        when 2 then 4
                        when 3 then 7
                        when 4 then 10
                        else 1
                    end as string
                ), 2, '0'),
                '-01'
            ) as date
        ) as report_date,
        product_name,
        concat('AQUATIC_', aquatic_type) as product_category_name,
        cast(value as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        unit as unit_name,
        null as quantity_unit_name,
        'GSO_EXCEL' as source_name,
        'QUARTERLY' as period_grain,
        ingest_at
    from silver.aquatic_products
),

gasoline as (
    select
        to_date(cast(`date` as string), 'dd-MM-yyyy') as report_date,
        trim(cast(product as string)) as product_name,
        concat('GASOLINE_', trim(cast(type as string))) as product_category_name,
        cast(price as decimal(38,10)) as value,
        cast(null as decimal(38,10)) as quantity,
        trim(cast(unit as string)) as unit_name,
        cast(null as string) as quantity_unit_name,
        trim(cast(source as string)) as source_name,
        cast('DAILY' as string) as period_grain,
        cast(processing_date as timestamp) as ingest_at
    from silver.gasoline
    where `date` is not null
      and price is not null
      and product is not null
)

select * from industry
union all select * from livestock
union all select * from forestry
union all select * from aquatic
union all select * from gasoline