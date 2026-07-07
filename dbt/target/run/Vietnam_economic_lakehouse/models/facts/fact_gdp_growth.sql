
        CREATE TABLE gold_gold.fact_gdp_growth
        USING DELTA
        AS
        

with base as (
    select
        cast(report_date as date) as report_date,
        cast(year(cast(report_date as date)) as int) as report_year,
        cast(quarter(cast(report_date as date)) as int) as report_quarter,
        sub_sector_name,
        unit_name as unit,
        max(case when lower(gdp_type) like '%current%' or lower(gdp_type) like '%market%' or lower(gdp_type) like '%hiện hành%' then cast(value as float) end) as market_value,
        max(case when lower(gdp_type) like '%constant%' or lower(gdp_type) like '%real%' or lower(gdp_type) like '%so sánh%' then cast(value as float) end) as constant_value
    from gold_staging.stg_gdp
    group by 1, 2, 3, 4, 5
),

joined as (
    select
        cast(t.time_key as int) as time_key,
        b.report_year as year,
        b.report_quarter as quarter,
        ss.sub_sector_key,
        ss.sector_key,
        b.unit,
        b.market_value,
        b.constant_value
    from base b
    left join gold_gold.dim_time t on b.report_date = t.full_date
    left join gold_gold.dim_sub_sector ss on b.sub_sector_name = ss.sub_sector_name
    where ss.sub_sector_key is not null 
      and t.time_key is not null
),

with_prev as (
    select
        cur.time_key,
        cur.year,
        cur.quarter,
        cur.sub_sector_key,
        cur.sector_key,
        cur.unit,
        cur.market_value,
        cur.constant_value,
        pq.market_value as market_value_pre_quarter,
        pq.constant_value as constant_value_pre_quarter,
        py.market_value as market_value_pre_year,
        py.constant_value as constant_value_pre_year
    from joined cur
    left join joined pq 
        on cur.sub_sector_key = pq.sub_sector_key 
        and cur.unit = pq.unit
        and (
            (cur.year = pq.year and cur.quarter = pq.quarter + 1) or
            (cur.year = pq.year + 1 and cur.quarter = 1 and pq.quarter = 4)
        )
    left join joined py 
        on cur.sub_sector_key = py.sub_sector_key 
        and cur.unit = py.unit
        and cur.year = py.year + 1 
        and cur.quarter = py.quarter
)

select 
    time_key,
    year,
    quarter,
    sub_sector_key,
    sector_key,
    cast(unit as string) as unit,
    cast(coalesce(market_value, 0) as float) as market_value,
    cast(coalesce(constant_value, 0) as float) as constant_value,
    cast(coalesce(market_value_pre_quarter, 0) as float) as market_value_pre_quarter,
    cast(coalesce(constant_value_pre_quarter, 0) as float) as constant_value_pre_quarter,
    cast(coalesce(market_value_pre_year, 0) as float) as market_value_pre_year,
    cast(coalesce(constant_value_pre_year, 0) as float) as constant_value_pre_year
from with_prev
    