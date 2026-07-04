
        CREATE TABLE gold_gold.fact_gdp
        USING DELTA
        AS
        

with base as (

    select
        cast(report_date as date) as report_date,

        cast(year(cast(report_date as date)) as int) as report_year,
        cast(quarter(cast(report_date as date)) as int) as report_quarter,

        sector_name,
        sub_sector_name,
        unit_name,

        max(
            case
                when lower(gdp_type) like '%current%'
                  or lower(gdp_type) like '%market%'
                  or lower(gdp_type) like '%hiện hành%'
                then cast(value as decimal(38,10))
            end
        ) as market_value,

        max(
            case
                when lower(gdp_type) like '%constant%'
                  or lower(gdp_type) like '%real%'
                  or lower(gdp_type) like '%so sánh%'
                then cast(value as decimal(38,10))
            end
        ) as constant_value,

        max(source_name) as source_name

    from gold_staging.stg_gdp

    group by
        cast(report_date as date),
        cast(year(cast(report_date as date)) as int),
        cast(quarter(cast(report_date as date)) as int),
        sector_name,
        sub_sector_name,
        unit_name

),

joined as (

    select
        cast(t.time_key as int) as time_key,

        b.report_year,
        b.report_quarter,

        ss.sub_sector_key,
        u.unit_key,
        coalesce(s.source_key, cast(-1 as bigint)) as source_key,

        b.market_value,
        b.constant_value

    from base b

    left join gold_gold.dim_time t
        on b.report_date = t.full_date

    left join gold_gold.dim_sub_sector ss
        on b.sub_sector_name = ss.sub_sector_name

    left join gold_gold.dim_unit u
        on b.unit_name = u.unit_name

    left join gold_gold.dim_source s
        on b.source_name = s.source_name

    where ss.sub_sector_key is not null
      and t.time_key is not null

),

with_prev as (

    select
        cur.time_key,
        cur.report_year,
        cur.report_quarter,

        cur.sub_sector_key,
        cur.unit_key,
        cur.source_key,

        cur.market_value,
        cur.constant_value,

        pq.market_value as market_value_pre_quarter,
        pq.constant_value as constant_value_pre_quarter,

        py.market_value as market_value_pre_year,
        py.constant_value as constant_value_pre_year

    from joined cur

    left join joined pq
        on cur.sub_sector_key = pq.sub_sector_key
        and cur.unit_key = pq.unit_key
        and cur.source_key <=> pq.source_key
        and (
            (
                cur.report_year = pq.report_year
                and cur.report_quarter = pq.report_quarter + 1
            )
            or
            (
                cur.report_year = pq.report_year + 1
                and cur.report_quarter = 1
                and pq.report_quarter = 4
            )
        )

    left join joined py
        on cur.sub_sector_key = py.sub_sector_key
        and cur.unit_key = py.unit_key
        and cur.source_key <=> py.source_key
        and cur.report_year = py.report_year + 1
        and cur.report_quarter = py.report_quarter

),

final as (

    select
        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(sub_sector_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_gdp_key,

        time_key,
        report_year,
        report_quarter,

        sub_sector_key,
        unit_key,
        source_key,

        cast(round(market_value, 3) as decimal(38,3)) as market_value,
        cast(round(constant_value, 3) as decimal(38,3)) as constant_value,

        cast(round(market_value_pre_quarter, 3) as decimal(38,3)) as market_value_pre_quarter,
        cast(round(constant_value_pre_quarter, 3) as decimal(38,3)) as constant_value_pre_quarter,

        cast(round(market_value_pre_year, 3) as decimal(38,3)) as market_value_pre_year,
        cast(round(constant_value_pre_year, 3) as decimal(38,3)) as constant_value_pre_year,

        
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(sub_sector_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
        current_timestamp() as created_at

    from with_prev

)

select *
from final
    