{{
    config(
        materialized='delta_table'
    )
}}

with base as (

    select
        cast(to_date(report_date) as date) as report_date,

        cast(year(to_date(report_date)) as int) as report_year,
        cast(quarter(to_date(report_date)) as int) as report_quarter,

        trim(capital_source_name) as capital_source_name,
        trim(unit_name) as unit_name,
        trim(source_name) as source_name,

        cast(investment_value as decimal(38,10)) as investment_value,
        period_grain,
        ingest_at

    from {{ ref('stg_social_total_investment') }}

    where report_date is not null
      and capital_source_name is not null
      and unit_name is not null
      and source_name is not null

),

joined as (

    select
        cast(dt.time_key as int) as time_key,

        b.report_date,
        b.report_year,
        b.report_quarter,

        cs.capital_source_key,
        u.unit_key,
        s.source_key,

        b.investment_value,
        b.period_grain,
        b.ingest_at

    from base b

    left join {{ ref('dim_time') }} dt
        on b.report_date = dt.full_date

    left join {{ ref('dim_capital_source') }} cs
        on b.capital_source_name = cs.source_name

    left join {{ ref('dim_unit') }} u
        on b.unit_name = u.unit_name

    left join {{ ref('dim_source') }} s
        on b.source_name = s.source_name

    where dt.time_key is not null
      and cs.capital_source_key is not null
      and u.unit_key is not null
      and s.source_key is not null

),

deduped as (

    select
        time_key,
        report_date,
        report_year,
        report_quarter,

        capital_source_key,
        unit_key,
        source_key,

        cast(max(investment_value) as decimal(38,10)) as investment_value,
        max(period_grain) as period_grain,
        max(ingest_at) as ingest_at

    from joined

    group by
        time_key,
        report_date,
        report_year,
        report_quarter,
        capital_source_key,
        unit_key,
        source_key

),

with_prev as (

    select
        cur.time_key,
        cur.report_date,
        cur.report_year,
        cur.report_quarter,

        cur.capital_source_key,
        cur.unit_key,
        cur.source_key,

        cur.investment_value,

        prev_quarter.investment_value as investment_value_pre_quarter,
        prev_year.investment_value as investment_value_pre_year,

        cur.period_grain,
        cur.ingest_at

    from deduped cur

    left join deduped prev_quarter
        on cur.capital_source_key = prev_quarter.capital_source_key
       and cur.unit_key = prev_quarter.unit_key
       and cur.source_key = prev_quarter.source_key
       and (
            (
                cur.report_year = prev_quarter.report_year
                and cur.report_quarter = prev_quarter.report_quarter + 1
            )
            or
            (
                cur.report_year = prev_quarter.report_year + 1
                and cur.report_quarter = 1
                and prev_quarter.report_quarter = 4
            )
       )

    left join deduped prev_year
        on cur.capital_source_key = prev_year.capital_source_key
       and cur.unit_key = prev_year.unit_key
       and cur.source_key = prev_year.source_key
       and cur.report_year = prev_year.report_year + 1
       and cur.report_quarter = prev_year.report_quarter

),

final as (

    select
        {{ sk(['time_key', 'capital_source_key', 'unit_key', 'source_key']) }} as fact_total_investment_key,

        time_key,
        capital_source_key,
        unit_key,
        source_key,

        cast(round(investment_value, 3) as decimal(38,3)) as investment_value,

        cast(
            round(investment_value_pre_quarter, 3)
            as decimal(38,3)
        ) as investment_value_pre_quarter,

        cast(
            round(investment_value_pre_year, 3)
            as decimal(38,3)
        ) as investment_value_pre_year,

        period_grain,

        {{ sk(['time_key', 'capital_source_key', 'unit_key', 'source_key']) }} as load_id,
        current_timestamp() as created_at,
        ingest_at

    from with_prev

)

select *
from final