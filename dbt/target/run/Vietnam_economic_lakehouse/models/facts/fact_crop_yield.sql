
        CREATE TABLE gold_gold.fact_crop_yield
        USING DELTA
        AS
        

with base as (
    select
        cast(t.time_key as int) as time_key,
        cast(a.report_year as int) as report_year,
        cast(c.crop_key as int) as crop_key,
        cast(c.crop_category as string) as crop_category,
        cast(a.area as float) as area,
        cast(a.production as float) as production,
        cast(a.source_yield as float) as yield,
        a.production_unit_name as production_unit,
        a.yield_unit_name as yield_unit,
        a.area_unit_name as area_unit
    from gold_staging.stg_crop a
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
        cur.crop_key,
        cur.report_year,
        cur.crop_category,
        cur.yield_unit as productivity_unit,   
        cur.production_unit as yield_unit,     
        cur.area_unit as area_unit,
        cur.area,
        cur.production as yield_value,
        cur.yield as productivity,
        pre.area as area_pre_year,
        pre.production as yield_pre_year,
        pre.yield as productivity_pre_year
    from base cur
    left join base pre
        on cur.crop_key = pre.crop_key
        and cur.report_year = pre.report_year + 1
)

select
    time_key,
    crop_key,
    report_year,         -- Giữ lại phục vụ Partition Window ở tầng Mart
    crop_category,       -- Giữ lại phục vụ Partition Window ở tầng Mart
    productivity_unit,
    yield_unit,
    area_unit,
    cast(coalesce(area, 0) as float) as area,
    cast(coalesce(round(yield_value, 3), 0) as float) as yield_value,
    cast(coalesce(productivity, 0) as float) as productivity,
    cast(coalesce(area_pre_year, 0) as float) as area_pre_year,
    cast(coalesce(yield_pre_year, 0) as float) as yield_pre_year,
    cast(coalesce(productivity_pre_year, 0) as float) as productivity_pre_year
from with_prev_year
    