{{
    config(
        materialized='delta_table'
    )
}}

with asset_enriched as (

    select
        a.asset_key,
        a.symbol,
        ac.asset_class_name,
        m.market_name

    from {{ ref('dim_asset') }} a

    left join {{ ref('dim_asset_class') }} ac
        on a.asset_class_key = ac.asset_class_key

    left join {{ ref('dim_market') }} m
        on a.market_key = m.market_key

),

joined as (

    select
        cast(dt.time_key as int) as time_key,

        a.asset_key,
        t.term_key,
        u.unit_key,
        coalesce(s.source_key, cast(-1 as bigint)) as source_key,

        cast(r.interest_rate as decimal(38,10)) as interest_rate,
        cast(r.volume as decimal(38,10)) as volume,

        case
            when r.interest_rate is not null
             and r.volume is not null
                then cast(r.interest_rate as decimal(38,10))
                   * cast(r.volume as decimal(38,10))
            else null
        end as rate_volume_amount

    from {{ ref('stg_interest_rate') }} r

    left join {{ ref('dim_time') }} dt
        on cast(r.date as date) = cast(dt.`full_date` as date)

    left join asset_enriched a
        on r.symbol = a.symbol
       and r.asset_class_name = a.asset_class_name
       and r.market_name = a.market_name

    left join {{ ref('dim_term') }} t
        on r.term_name = t.term_name

    left join {{ ref('dim_unit') }} u
        on r.unit_name = u.unit_name

    left join {{ ref('dim_source') }} s
        on r.source_name = s.source_name

)

select
    {{ sk(['time_key', 'asset_key', 'term_key', 'unit_key', 'source_key']) }} as fact_interest_key,

    time_key,
    asset_key,
    term_key,
    source_key,
    unit_key,

    cast(round(interest_rate, 6) as decimal(38,6)) as interest_rate,
    cast(round(volume, 3) as decimal(38,3)) as volume,

    cast(round(rate_volume_amount, 6) as decimal(38,6)) as rate_volume_amount,

    {{ sk(['time_key', 'asset_key', 'term_key', 'unit_key', 'source_key']) }} as load_id,
    current_timestamp() as created_at

from joined

where asset_key is not null
  and time_key is not null