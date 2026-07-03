{{
    config(
        materialized='delta_table'
    )
}}

with base as (

    select
        f.fact_key,

        f.time_key,
        t.full_date as report_date,
        cast(t.year as int) as report_year,
        cast(t.quarter as int) as report_quarter,
        cast(t.month as int) as report_month,
        cast(t.day as int) as report_day,

        f.indicator_key,
        i.indicator_name,

        f.unit_key,
        u.unit_name,
        u.unit_nor,

        f.source_key,
        s.source_name,
        s.source_system,

        upper(trim(f.period_grain)) as period_grain,

        cast(f.value as decimal(38,6)) as value,

        f.created_at,
        f.ingest_at

    from {{ ref('fact_macro_indicator') }} f

    left join {{ ref('dim_time') }} t
        on f.time_key = t.time_key

    left join {{ ref('dim_indicator') }} i
        on f.indicator_key = i.indicator_key

    left join {{ ref('dim_unit') }} u
        on f.unit_key = u.unit_key

    left join {{ ref('dim_source') }} s
        on f.source_key = s.source_key

),

with_prev as (

    select
        cur.fact_key,

        cur.time_key,
        cur.report_date,
        cur.report_year,
        cur.report_quarter,
        cur.report_month,
        cur.report_day,

        cur.indicator_key,
        cur.indicator_name,

        cur.unit_key,
        cur.unit_name,
        cur.unit_nor,

        cur.source_key,
        cur.source_name,
        cur.source_system,

        cur.period_grain,

        cur.value,

        prev_period.value as value_pre_period,
        prev_year.value as value_pre_year,

        cur.created_at,
        cur.ingest_at

    from base cur

    left join base prev_period
        on cur.indicator_key = prev_period.indicator_key
       and cur.unit_key = prev_period.unit_key
       and cur.source_key = prev_period.source_key
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

    left join base prev_year
        on cur.indicator_key = prev_year.indicator_key
       and cur.unit_key = prev_year.unit_key
       and cur.source_key = prev_year.source_key
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

final as (

    select
        fact_key as mart_macro_indicator_key,

        time_key,
        report_date,
        report_year,
        report_quarter,
        report_month,
        report_day,

        indicator_key,
        indicator_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        period_grain,

        value,

        cast(round(value_pre_period, 6) as decimal(38,6)) as value_pre_period,
        cast(round(value_pre_year, 6) as decimal(38,6)) as value_pre_year,

        cast(
            round(
                value - value_pre_period,
                6
            ) as decimal(38,6)
        ) as period_change,

        cast(
            round(
                {{ safe_divide('value - value_pre_period', 'value_pre_period') }} * 100,
                6
            ) as decimal(38,6)
        ) as period_growth_rate,

        cast(
            round(
                value - value_pre_year,
                6
            ) as decimal(38,6)
        ) as yoy_change,

        cast(
            round(
                {{ safe_divide('value - value_pre_year', 'value_pre_year') }} * 100,
                6
            ) as decimal(38,6)
        ) as yoy_growth_rate,

        created_at,
        ingest_at

    from with_prev

)

select *
from final