
        CREATE TABLE gold_marts.mart_agriculture_metrics
        USING DELTA
        AS
        

with base as (

    select
        f.time_key,
        t.full_date,
        cast(t.year as int) as report_year,

        f.crop_key,
        c.crop_name,
        cg.crop_group_name,

        f.source_key,
        s.source_name,
        s.source_system,

        f.production,
        f.area,
        f.yield_value,
        f.source_yield,

        f.production_pre_year,
        f.area_pre_year,
        f.yield_pre_year,

        f.production_unit_name as production_unit_name,
        f.area_unit_name as area_unit_name,
        f.yield_unit_name as yield_unit_name,

        f.period_grain,
        f.created_at,
        f.ingest_at

    from gold_gold.fact_agriculture f

    left join gold_gold.dim_time t
        on f.time_key = t.time_key

    left join gold_gold.dim_crop c
        on f.crop_key = c.crop_key

    left join gold_gold.dim_crop_group cg
        on c.crop_group_key = cg.crop_group_key

    left join gold_gold.dim_source s
        on f.source_key = s.source_key

),

calculated as (

    select
        time_key,
        full_date,
        report_year,

        crop_key,
        crop_name,
        crop_group_name,

        source_key,
        source_name,
        source_system,

        production_unit_name,
        area_unit_name,
        yield_unit_name,

        cast(production as decimal(38,3)) as production_value,
        cast(production_pre_year as decimal(38,3)) as production_value_pre_year,

        cast(area as decimal(38,3)) as area_value,
        cast(area_pre_year as decimal(38,3)) as area_value_pre_year,

        cast(yield_value as decimal(38,3)) as yield_value,
        cast(yield_pre_year as decimal(38,3)) as yield_value_pre_year,

        cast(yield_value as decimal(38,3)) as productivity_value,
        cast(yield_pre_year as decimal(38,3)) as productivity_value_pre_year,

        cast(source_yield as decimal(38,3)) as source_yield_value,

        period_grain,
        created_at,
        ingest_at

    from base

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(crop_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as mart_crop_yield_key,

        time_key,
        full_date,
        report_year,

        crop_key,
        crop_name,
        crop_group_name,

        source_key,
        source_name,
        source_system,

        production_unit_name,
        area_unit_name,
        yield_unit_name,

        production_value,
        production_value_pre_year,

        area_value,
        area_value_pre_year,

        yield_value,
        yield_value_pre_year,

        productivity_value,
        productivity_value_pre_year,

        source_yield_value,

        cast(
            round(
                
    production_value - production_value_pre_year / nullif(production_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as production_yoy_growth_rate,

        cast(
            round(
                
    area_value - area_value_pre_year / nullif(area_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as area_yoy_growth_rate,

        cast(
            round(
                
    yield_value - yield_value_pre_year / nullif(yield_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as yield_yoy_growth_rate,

        cast(
            round(
                
    productivity_value - productivity_value_pre_year / nullif(productivity_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as productivity_yoy_growth_rate,

        cast(
            round(
                
    yield_value - yield_value_pre_year / nullif(yield_value_pre_year, 0)
 * 100,
                3
            ) as decimal(38,3)
        ) as value_yoy_growth_rate,

        period_grain,
        created_at,
        ingest_at

    from calculated

)

select *
from final
    