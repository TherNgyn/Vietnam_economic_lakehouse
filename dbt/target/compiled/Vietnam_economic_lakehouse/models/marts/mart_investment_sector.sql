

with base as (

    select
        f.fact_investment_by_sector_key,

        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,

        f.sector_key,
        ds.sector_name,

        f.unit_key,
        u.unit_name,
        u.unit_nor,

        f.source_key,
        s.source_name,
        s.source_system,

        cast(f.investment_value as decimal(38,3)) as investment_value,
        cast(f.investment_value_pre_year as decimal(38,3)) as investment_value_pre_year,

        f.created_at,
        f.ingest_at

    from gold_gold.fact_investment_by_sector f

    left join gold_gold.dim_time t
        on f.time_key = t.time_key

    left join gold_gold.dim_sector ds
        on f.sector_key = ds.sector_key

    left join gold_gold.dim_unit u
        on f.unit_key = u.unit_key

    left join gold_gold.dim_source s
        on f.source_key = s.source_key

),

with_growth as (

    select
        fact_investment_by_sector_key,

        time_key,
        full_date,
        report_year,

        sector_key,
        sector_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        investment_value,
        investment_value_pre_year,

        cast(
            round(
                investment_value - investment_value_pre_year,
                3
            ) as decimal(38,3)
        ) as investment_yoy_change,

        cast(
            round(
                
    investment_value - investment_value_pre_year / nullif(investment_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as investment_yoy_growth_rate,

        created_at,
        ingest_at

    from base

),

with_totals as (

    select
        *,

        sum(investment_value) over (
            partition by time_key, unit_key, source_key
        ) as total_investment_value

    from with_growth

),

with_ranking as (

    select
        *,

        rank() over (
            partition by time_key, unit_key, source_key
            order by investment_value desc
        ) as investment_rank,

        rank() over (
            partition by time_key, unit_key, source_key
            order by investment_yoy_growth_rate desc
        ) as investment_growth_rank

    from with_totals

),

final as (

    select
        fact_investment_by_sector_key as mart_investment_by_sector_growth_key,

        time_key,
        full_date,
        report_year,

        sector_key,
        sector_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        investment_value,
        investment_value_pre_year,

        investment_yoy_change,
        investment_yoy_growth_rate,

        cast(
            round(
                
    investment_value / nullif(total_investment_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as sector_investment_share_pct,

        investment_rank,
        investment_growth_rank,

        created_at,
        ingest_at

    from with_ranking

)

select *
from final