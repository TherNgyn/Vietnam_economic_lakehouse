{{ config(
    materialized='delta_table'
) }}

with base as (
    select
        cast(to_date(report_date) as date) as report_date,
        cast(year(to_date(report_date)) as int) as report_year,
        cast(quarter(to_date(report_date)) as int) as report_quarter,
        trim(capital_source_name) as capital_source_name,
        cast(unit_name as string) as unit,
        cast(investment_value as decimal(38,10)) as investment_value,
        period_grain,
        ingest_at
    from {{ ref('stg_social_total_investment') }}
    where report_date is not null
      and capital_source_name is not null
),

joined as (
    select
        cast(dt.time_key as int) as time_key,
        b.report_date,
        b.report_year,
        b.report_quarter,
        cs.capital_source_key,
        b.unit,
        b.investment_value,
        b.period_grain,
        b.ingest_at
    from base b
    
    left join {{ ref('dim_time') }} dt
        on b.report_date = dt.full_date
    left join {{ ref('dim_capital_source') }} cs
        on b.capital_source_name = cs.source_name
    where dt.time_key is not null
      and cs.capital_source_key is not null
),

deduped as (
    select
        time_key,
        report_date,
        report_year,
        report_quarter,
        capital_source_key,
        unit,
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
        unit
),

with_prev_quarter as (
    select
        cur.time_key,
        cur.report_date,
        cur.report_year,
        cur.report_quarter,
        cur.capital_source_key,
        cur.unit,
        cur.investment_value,
        prev.investment_value as investment_value_pre_quarter,
        cur.period_grain,
        cur.ingest_at
    from deduped cur
    left join deduped prev
        on cur.capital_source_key = prev.capital_source_key
       and cur.unit = prev.unit
       and (
            (
                cur.report_year = prev.report_year
                and cur.report_quarter = prev.report_quarter + 1
            )
            or
            (
                cur.report_year = prev.report_year + 1
                and cur.report_quarter = 1
                and prev.report_quarter = 4
            )
       )
),

with_prev_year as (
    select
        cur.time_key,
        cur.report_date,
        cur.report_year,
        cur.report_quarter,
        cur.capital_source_key,
        cur.unit,
        cur.investment_value,
        cur.investment_value_pre_quarter,
        prev.investment_value as investment_value_pre_year,
        cur.period_grain,
        cur.ingest_at
    from with_prev_quarter cur
    left join deduped prev
        on cur.capital_source_key = prev.capital_source_key
       and cur.unit = prev.unit
       and cur.report_year = prev.report_year + 1
       and cur.report_quarter = prev.report_quarter
),

final as (
    select
        {{ sk(['time_key', 'capital_source_key', 'unit']) }} as fact_total_investment_key,

        cast(time_key as int) as time_key,
        cast(capital_source_key as int) as capital_source_key,
        cast(unit as string) as unit,

        cast(coalesce(investment_value, 0) as float) as investment_value,
        cast(coalesce(investment_value_pre_quarter, 0) as float) as investment_value_pre_quarter,
        cast(coalesce(investment_value_pre_year, 0) as float) as investment_value_pre_year,

        period_grain,
        current_timestamp() as created_at,
        ingest_at
    from with_prev_year
)

select * from final