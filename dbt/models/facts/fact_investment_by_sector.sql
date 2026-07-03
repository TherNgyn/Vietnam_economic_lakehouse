{{
    config(
        materialized='delta_table'
    )
}}

with base as (
    select
        cast(to_date(report_date) as date) as report_date,
        sector_name,
        unit_name,
        source_name,
        cast(value as decimal(38,10)) as investment_value,
        ingest_at
    from {{ ref('stg_investment_by_sector') }}

),

with_pre_year as (
    select
        b.report_date,
        b.sector_name,
        b.unit_name,
        b.source_name,
        b.investment_value,
        cast(p.investment_value as decimal(38,10)) as investment_value_pre_year,
        b.ingest_at

    from base b

    left join base p
        on b.sector_name = p.sector_name
       and coalesce(b.unit_name, '') = coalesce(p.unit_name, '')
       and coalesce(b.source_name, '') = coalesce(p.source_name, '')
       and p.report_date = add_months(b.report_date, -12)

),

joined as (

    select
        cast(dt.time_key as int) as time_key,

        ds.sector_key,
        u.unit_key,
        s.source_key,

        b.investment_value,
        b.investment_value_pre_year,
        b.ingest_at

    from with_pre_year b

    left join {{ ref('dim_time') }} dt
        on b.report_date = dt.full_date

    left join {{ ref('dim_sector') }} ds
        on b.sector_name = ds.sector_name

    left join {{ ref('dim_unit') }} u
        on b.unit_name = u.unit_name

    left join {{ ref('dim_source') }} s
        on b.source_name = s.source_name

    where dt.time_key is not null
      and ds.sector_key is not null
      and u.unit_key is not null
      and s.source_key is not null

),

final as (

    select
        {{ sk(['time_key', 'sector_key', 'unit_key', 'source_key']) }} as fact_investment_by_sector_key,

        time_key,
        sector_key,
        unit_key,
        source_key,

        cast(round(investment_value, 3) as decimal(38,3)) as investment_value,

        cast(
            round(investment_value_pre_year, 3)
            as decimal(38,3)
        ) as investment_value_pre_year,

        {{ sk(['time_key', 'sector_key', 'unit_key', 'source_key']) }} as load_id,
        current_timestamp() as created_at,
        ingest_at

    from joined

)

select *
from final