
        CREATE TABLE gold_marts.mart_gdp_metrics
        USING DELTA
        AS
        

with base as (

    select
        f.fact_gdp_key,

        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,
        cast(t.quarter as int) as report_quarter,

        f.sub_sector_key,
        ss.sub_sector_name,

        s.sector_key,
        s.sector_name,

        f.unit_key,
        u.unit_name,
        u.unit_nor,

        f.source_key,
        src.source_name,
        src.source_system,

        cast(f.market_value as decimal(38,3)) as market_value,
        cast(f.constant_value as decimal(38,3)) as constant_value,

        cast(f.market_value_pre_quarter as decimal(38,3)) as market_value_pre_quarter,
        cast(f.constant_value_pre_quarter as decimal(38,3)) as constant_value_pre_quarter,

        cast(f.market_value_pre_year as decimal(38,3)) as market_value_pre_year,
        cast(f.constant_value_pre_year as decimal(38,3)) as constant_value_pre_year,

        f.created_at

    from gold_gold.fact_gdp f

    left join gold_gold.dim_time t
        on f.time_key = t.time_key

    left join gold_gold.dim_sub_sector ss
        on f.sub_sector_key = ss.sub_sector_key

    left join gold_gold.dim_sector s
        on ss.sector_key = s.sector_key

    left join gold_gold.dim_unit u
        on f.unit_key = u.unit_key

    left join gold_gold.dim_source src
        on f.source_key = src.source_key

),


with_growth as (

    select
        fact_gdp_key,

        time_key,
        full_date,
        report_year,
        report_quarter,

        sector_key,
        sector_name,

        sub_sector_key,
        sub_sector_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        market_value,
        constant_value,

        market_value_pre_quarter,
        constant_value_pre_quarter,

        market_value_pre_year,
        constant_value_pre_year,

        cast(
            case
                when market_value_pre_quarter is null then null
                else round(market_value - market_value_pre_quarter, 3)
            end as decimal(38,3)
        ) as market_qoq_growth_value,

        cast(
            case
                when market_value_pre_quarter is null
                  or abs(market_value_pre_quarter) < 0.001
                then null
                else round(
                    (market_value - market_value_pre_quarter)
                    / market_value_pre_quarter * 100,
                    3
                )
            end as decimal(38,3)
        ) as market_qoq_growth_rate,

        cast(
            case
                when market_value_pre_year is null then null
                else round(market_value - market_value_pre_year, 3)
            end as decimal(38,3)
        ) as market_yoy_growth_value,

        cast(
            case
                when market_value_pre_year is null
                  or abs(market_value_pre_year) < 0.001
                then null
                else round(
                    (market_value - market_value_pre_year)
                    / market_value_pre_year * 100,
                    3
                )
            end as decimal(38,3)
        ) as market_yoy_growth_rate,

        cast(
            case
                when constant_value_pre_quarter is null then null
                else round(constant_value - constant_value_pre_quarter, 3)
            end as decimal(38,3)
        ) as real_qoq_growth_value,

        cast(
            case
                when constant_value_pre_quarter is null
                  or abs(constant_value_pre_quarter) < 0.001
                then null
                else round(
                    (constant_value - constant_value_pre_quarter)
                    / constant_value_pre_quarter * 100,
                    3
                )
            end as decimal(38,3)
        ) as real_qoq_growth_rate,

        cast(
            case
                when constant_value_pre_year is null then null
                else round(constant_value - constant_value_pre_year, 3)
            end as decimal(38,3)
        ) as real_yoy_growth_value,

        cast(
            case
                when constant_value_pre_year is null
                  or abs(constant_value_pre_year) < 0.001
                then null
                else round(
                    (constant_value - constant_value_pre_year)
                    / constant_value_pre_year * 100,
                    3
                )
            end as decimal(38,3)
        ) as real_yoy_growth_rate,

        cast(
            case
                when constant_value is null
                  or abs(constant_value) < 0.001
                then null
                else round(market_value / constant_value * 100, 3)
            end as decimal(38,3)
        ) as implicit_price_deflator,

        created_at

    from base

),


with_totals as (

    select
        *,

        sum(market_value) over (
            partition by time_key, sector_key
        ) as sector_total_market_value,

        sum(market_value) over (
            partition by time_key
        ) as gdp_total_market_value

    from with_growth

),

final as (

    select
        fact_gdp_key as mart_gdp_growth_key,

        time_key,
        full_date,
        report_year,
        report_quarter,

        sector_key,
        sector_name,

        sub_sector_key,
        sub_sector_name,

        unit_key,
        unit_name,
        unit_nor,

        source_key,
        source_name,
        source_system,

        market_value,
        constant_value,

        market_value_pre_quarter,
        constant_value_pre_quarter,

        market_value_pre_year,
        constant_value_pre_year,

        market_qoq_growth_value,
        market_qoq_growth_rate,

        market_yoy_growth_value,
        market_yoy_growth_rate,

        real_qoq_growth_value,
        real_qoq_growth_rate,

        real_yoy_growth_value,
        real_yoy_growth_rate,

        implicit_price_deflator,

        cast(
            round(
                
    market_value / nullif(sector_total_market_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as sector_share_pct,

        cast(
            round(
                
    market_value / nullif(gdp_total_market_value, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as gdp_share_pct,

        created_at

    from with_totals

)

select *
from final
    