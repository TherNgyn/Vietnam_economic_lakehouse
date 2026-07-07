create or replace view gold_staging.stg_production_output
  
  
  as
    

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
        'Industry Product' as product_category_name,
        cast(value as float) as value,
        unit as unit_name,
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
                lpad(cast(
                    case cast(`quarter` as int)
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
        trim(cast(livestock_indicator as string)) as product_name,
        cast('Livestock Product' as string) as product_category_name,
        cast(value as float) as value,
        trim(cast(unit as string)) as unit_name,
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
                lpad(cast(
                    case cast(`quarter` as int)
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
        trim(cast(forestry_indicator as string)) as product_name,
        cast('Forestry Product' as string) as product_category_name,
        cast(value as float) as value,
        trim(cast(unit as string)) as unit_name,
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
        concat('Aquatic Product', aquatic_type) as product_category_name,
        cast(value as float) as value,
        unit as unit_name,
        'GSO_EXCEL' as source_name,
        'QUARTERLY' as period_grain,
        ingest_at
    from silver.aquatic_products
)

select * from industry
union all select * from livestock
union all select * from forestry
union all select * from aquatic
