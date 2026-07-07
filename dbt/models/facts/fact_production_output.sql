{{ config(
    materialized='delta_table'
) }}

with base as (
    select
        cast(report_date as date) as report_date,
        cast(year(report_date) as int) as report_year,
        cast(quarter(report_date) as int) as report_quarter,
        cast(month(report_date) as int) as report_month,
        cast(day(report_date) as int) as report_day,
        product_name,
        product_category_name,
        -- Khôi phục trường giá trị mặc định của product_type giống như đã quy ước ở Dim
        case 
            when product_category_name like 'GASOLINE_%' then 'Gasoline Product'
            else 'not available' 
        end as product_type,
        value,
        unit_name as unit,
        period_grain,
        ingest_at
    from {{ ref('stg_production_output') }}
    where product_name is not null
      and product_category_name is not null
      and report_date is not null
      and value is not null
),

joined as (
    select
        cast(dt.time_key as int) as time_key,
        p.product_key, -- Lấy Key tổng hợp từ dim_product mới
        b.report_date,
        b.report_year,
        b.report_quarter,
        b.report_month,
        b.report_day,
        b.period_grain,
        b.value,
        b.unit,
        b.ingest_at
    from base b
    left join {{ ref('dim_time') }} dt 
        on b.report_date = dt.full_date
    left join {{ ref('dim_product') }} p 
        on b.product_name = p.product_name 
       and b.product_category_name = p.product_category_name
       and b.product_type = p.product_type
    where dt.time_key is not null
      and p.product_key is not null
),

joined_dedup as (
    select
        time_key, product_key, period_grain,
        report_date, report_year, report_quarter, report_month, report_day,
        max(value) as value,
        max(unit) as unit,
        max(ingest_at) as ingest_at
    from joined
    group by time_key, product_key, period_grain, report_date, report_year, report_quarter, report_month, report_day
),

with_prev as (
    select
        cur.time_key, cur.product_key, cur.period_grain,
        cur.report_year, cur.report_quarter, cur.report_month,
        cur.value, cur.unit,
        prev_period.value as prev_period_value,
        prev_year.value as pre_year_value,
        cur.ingest_at
    from joined_dedup cur
    left join joined_dedup prev_period
        on cur.product_key = prev_period.product_key
       and cur.period_grain = prev_period.period_grain
       and (
            (cur.period_grain = 'DAILY' and cur.report_date = date_add(prev_period.report_date, 1))
            or
            (cur.period_grain = 'MONTHLY' and ((cur.report_year = prev_period.report_year and cur.report_month = prev_period.report_month + 1) or (cur.report_year = prev_period.report_year + 1 and cur.report_month = 1 and prev_period.report_month = 12)))
            or
            (cur.period_grain = 'QUARTERLY' and ((cur.report_year = prev_period.report_year and cur.report_quarter = prev_period.report_quarter + 1) or (cur.report_year = prev_period.report_year + 1 and cur.report_quarter = 1 and prev_period.report_quarter = 4)))
            or
            (cur.period_grain = 'YEARLY' and cur.report_year = prev_period.report_year + 1)
       )
    left join joined_dedup prev_year
        on cur.product_key = prev_year.product_key
       and cur.period_grain = prev_year.period_grain
       and cur.report_year = prev_year.report_year + 1
       and (
            (cur.period_grain = 'DAILY' and cur.report_month = prev_year.report_month and cur.report_day = prev_year.report_day)
            or
            (cur.period_grain = 'MONTHLY' and cur.report_month = prev_year.report_month)
            or
            (cur.period_grain = 'QUARTERLY' and cur.report_quarter = prev_year.report_quarter)
            or
            (cur.period_grain = 'YEARLY')
       )
),

final_dedup as (
    select
        time_key, product_key, period_grain,
        max(report_year) as report_year,
        max(report_quarter) as report_quarter,
        max(report_month) as report_month,
        max(value) as value,
        max(unit) as unit,
        max(prev_period_value) as prev_period_value,
        max(pre_year_value) as pre_year_value,
        max(ingest_at) as ingest_at
    from with_prev
    group by time_key, product_key, period_grain
)

select
    {{ sk(['time_key', 'product_key', 'period_grain']) }} as fact_production_key,
    time_key,
    product_key,
    period_grain,
    report_year,
    report_quarter,
    report_month,
    cast(coalesce(value, 0) as float) as value,
    cast(unit as string) as unit,
    cast(coalesce(prev_period_value, 0) as float) as prev_period_value,
    cast(coalesce(pre_year_value, 0) as float) as pre_year_value,
    ingest_at
from final_dedup