

with base as (

    select
        cast(to_date(report_date) as date) as report_date,

        cast(year(to_date(report_date)) as int) as report_year,
        cast(quarter(to_date(report_date)) as int) as report_quarter,
        cast(month(to_date(report_date)) as int) as report_month,
        cast(day(to_date(report_date)) as int) as report_day,

        trim(cast(product_name as string)) as product_name,
        trim(cast(product_category_name as string)) as product_category_name,

        cast(value as decimal(38,10)) as value,
        trim(cast(unit_name as string)) as unit,

        trim(cast(period_grain as string)) as period_grain,
        cast(ingest_at as timestamp) as ingest_at

    from gold_staging.stg_production_output

    where product_name is not null
      and product_category_name is not null
      and report_date is not null
      and value is not null

),

joined as (

    select
        cast(dt.time_key as int) as time_key,

        p.product_key,
        pc.product_category_key,

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

    left join gold_gold.dim_time dt
        on b.report_date = dt.full_date

    left join gold_gold.dim_product_category pc
        on b.product_category_name = pc.product_category_name

    left join gold_gold.dim_product p
        on b.product_name = p.product_name
       and p.product_category_key = pc.product_category_key

    where dt.time_key is not null
      and pc.product_category_key is not null
      and p.product_key is not null

),

joined_dedup as (

    select
        time_key,
        product_key,
        product_category_key,
        period_grain,

        report_date,
        report_year,
        report_quarter,
        report_month,
        report_day,

        max(value) as value,
        max(unit) as unit,
        max(ingest_at) as ingest_at

    from joined

    group by
        time_key,
        product_key,
        product_category_key,
        period_grain,
        report_date,
        report_year,
        report_quarter,
        report_month,
        report_day

),

with_prev as (

    select
        cur.time_key,
        cur.product_key,
        cur.product_category_key,
        cur.period_grain,

        cur.report_date,
        cur.report_year,
        cur.report_quarter,
        cur.report_month,
        cur.report_day,

        cur.value,
        cur.unit,

        prev_period.value as prev_period_value,
        prev_year.value as pre_year_value,

        cur.ingest_at

    from joined_dedup cur

    left join joined_dedup prev_period
        on cur.product_key = prev_period.product_key
       and cur.product_category_key = prev_period.product_category_key
       and cur.period_grain = prev_period.period_grain
       and (
            (
                cur.period_grain = 'DAILY'
                and cur.report_date = date_add(prev_period.report_date, 1)
            )
            or
            (
                cur.period_grain = 'MONTHLY'
                and (
                    (
                        cur.report_year = prev_period.report_year
                        and cur.report_month = prev_period.report_month + 1
                    )
                    or
                    (
                        cur.report_year = prev_period.report_year + 1
                        and cur.report_month = 1
                        and prev_period.report_month = 12
                    )
                )
            )
            or
            (
                cur.period_grain = 'QUARTERLY'
                and (
                    (
                        cur.report_year = prev_period.report_year
                        and cur.report_quarter = prev_period.report_quarter + 1
                    )
                    or
                    (
                        cur.report_year = prev_period.report_year + 1
                        and cur.report_quarter = 1
                        and prev_period.report_quarter = 4
                    )
                )
            )
            or
            (
                cur.period_grain = 'YEARLY'
                and cur.report_year = prev_period.report_year + 1
            )
       )

    left join joined_dedup prev_year
        on cur.product_key = prev_year.product_key
       and cur.product_category_key = prev_year.product_category_key
       and cur.period_grain = prev_year.period_grain
       and cur.report_year = prev_year.report_year + 1
       and (
            (
                cur.period_grain = 'DAILY'
                and cur.report_month = prev_year.report_month
                and cur.report_day = prev_year.report_day
            )
            or
            (
                cur.period_grain = 'MONTHLY'
                and cur.report_month = prev_year.report_month
            )
            or
            (
                cur.period_grain = 'QUARTERLY'
                and cur.report_quarter = prev_year.report_quarter
            )
            or
            (
                cur.period_grain = 'YEARLY'
            )
       )

),

final_dedup as (

    select
        time_key,
        product_key,
        product_category_key,
        period_grain,

        max(value) as value,
        max(unit) as unit,

        max(prev_period_value) as prev_period_value,
        max(pre_year_value) as pre_year_value,

        max(ingest_at) as ingest_at

    from with_prev

    group by
        time_key,
        product_key,
        product_category_key,
        period_grain

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(product_key as string), '__null__'), coalesce(cast(product_category_key as string), '__null__'), coalesce(cast(period_grain as string), '__null__')))
 as fact_production_key,

        time_key,
        product_key,
        product_category_key,
        period_grain,

        cast(round(value, 3) as decimal(38,3)) as value,
        cast(unit as string) as unit,

        cast(round(prev_period_value, 3) as decimal(38,3)) as prev_period_value,
        cast(round(pre_year_value, 3) as decimal(38,3)) as pre_year_value,

        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(product_key as string), '__null__'), coalesce(cast(product_category_key as string), '__null__'), coalesce(cast(period_grain as string), '__null__')))
 as load_id,
        current_timestamp() as created_at,
        ingest_at

    from final_dedup

)

select *
from final