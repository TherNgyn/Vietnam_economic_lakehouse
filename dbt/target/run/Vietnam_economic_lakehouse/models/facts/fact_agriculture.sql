
        CREATE TABLE gold_gold.fact_agriculture
        USING DELTA
        AS
        

with base as (

    select
        cast(t.time_key as int) as time_key,
        cast(a.report_year as int) as report_year,
        cast(c.crop_key as bigint) as crop_key,
        cast(-1 as bigint) as source_key,

        cast(a.production as decimal(38,10)) as production,
        cast(a.area as decimal(38,10)) as area,

        case
            when a.area is not null and a.area > 0
                then cast(a.production as decimal(38,10)) / cast(a.area as decimal(38,10))
            else null
        end as yield_value,

        cast(a.source_yield as decimal(38,10)) as source_yield,

        a.production_unit_name,
        a.area_unit_name,
        a.yield_unit_name,
        a.period_grain,
        a.ingest_at

    from gold_staging.stg_agriculture a

    left join gold_gold.dim_crop c
        on a.crop_name = c.crop_name

    left join gold_gold.dim_time t
        on cast(concat(cast(a.report_year as string), '-01-01') as date) = t.full_date


    where c.crop_key is not null
      and t.time_key is not null

),

with_prev_year as (

    select
        cur.time_key,
        cur.report_year,
        cur.crop_key,
        cur.source_key,

        cur.production,
        cur.area,
        cur.yield_value,
        cur.source_yield,

        pre.production as production_pre_year,
        pre.area as area_pre_year,
        pre.yield_value as yield_pre_year,

        cur.production_unit_name,
        cur.area_unit_name,
        cur.yield_unit_name,
        cur.period_grain,
        cur.ingest_at

    from base cur

    left join base pre
        on cur.crop_key = pre.crop_key
        and cur.source_key = pre.source_key
        and cur.report_year = pre.report_year + 1

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(crop_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_agri_key,

        time_key,
        report_year,
        crop_key,
        source_key,

        cast(round(production, 3) as decimal(38,3)) as production,
        cast(round(area, 3) as decimal(38,3)) as area,
        cast(round(yield_value, 3) as decimal(38,3)) as yield_value,
        cast(round(source_yield, 3) as decimal(38,3)) as source_yield,

        cast(round(production_pre_year, 3) as decimal(38,3)) as production_pre_year,
        cast(round(area_pre_year, 3) as decimal(38,3)) as area_pre_year,
        cast(round(yield_pre_year, 3) as decimal(38,3)) as yield_pre_year,

        production_unit_name,
        area_unit_name,
        yield_unit_name,
        period_grain,

        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(crop_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
        current_timestamp() as created_at,
        ingest_at

    from with_prev_year

)

select *
from final
    