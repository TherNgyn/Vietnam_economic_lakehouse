

with base as (
    select
        report_date,
        sector_name,
        sub_sector_name,
        unit_name,

        max(
            case
                when lower(gdp_type) like '%current%'
                  or lower(gdp_type) like '%market%'
                  or lower(gdp_type) like '%hiện hành%'
                then value
            end
        ) as market_value,

        max(
            case
                when lower(gdp_type) like '%constant%'
                  or lower(gdp_type) like '%real%'
                  or lower(gdp_type) like '%so sánh%'
                then value
            end
        ) as constant_value,

        max(source_name) as source_name

    from gold_staging.stg_gdp

    group by
        report_date,
        sector_name,
        sub_sector_name,
        unit_name
),

joined as (
    select
        cast(date_format(cast(b.report_date as timestamp), 'yyyyMMdd') as int) as time_key,
        ss.sub_sector_key,
        u.unit_key,
        s.source_key,
        b.market_value,
        b.constant_value

    from base b

    left join gold_gold.dim_sub_sector ss
        on b.sub_sector_name = ss.sub_sector_name

    left join gold_gold.dim_unit u
        on b.unit_name = u.unit_name

    left join gold_gold.dim_source s
        on b.source_name = s.source_name
)

select
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(sub_sector_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as fact_gdp_key,
    time_key,
    sub_sector_key,
    unit_key,
    source_key,
    market_value,
    constant_value,
    
    abs(xxhash64(coalesce(cast(time_key as string), '__null__'), coalesce(cast(sub_sector_key as string), '__null__'), coalesce(cast(unit_key as string), '__null__'), coalesce(cast(source_key as string), '__null__')))
 as load_id,
    current_timestamp() as created_at

from joined

where sub_sector_key is not null