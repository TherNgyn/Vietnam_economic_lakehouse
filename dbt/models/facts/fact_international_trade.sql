{{
    config(
        materialized='delta_table'
    )
}}

with base as (

    select
        product_name,

        -- Mapping product_type của trade thành product_category_name
        -- Nếu stg_trade_international đã có product_category_name thì dùng trực tiếp cột đó.
        product_type as product_category_name,

        cast(trade_value as decimal(38,10)) as trade_value,
        cast(quantity as decimal(38,10)) as quantity,

        value_unit,
        quantity_unit,

        cast(year as int) as year,
        cast(quarter as int) as quarter,
        cast(month as int) as month,

        ingest_at

    from {{ ref('stg_trade_international') }}

),

joined as (

    select
        t.time_key,
        p.product_key,

        b.year,
        b.quarter,
        b.month,

        b.trade_value,
        b.quantity,

        b.value_unit,
        b.quantity_unit,

        b.ingest_at

    from base b

    left join {{ ref('dim_product_category') }} pc
        on b.product_category_name = pc.product_category_name

    left join {{ ref('dim_product') }} p
        on b.product_name = p.product_name
       and p.product_category_key = pc.product_category_key

    left join {{ ref('dim_time') }} t
        on b.year = cast(t.year as int)
       and b.quarter = cast(t.quarter as int)
       and b.month = cast(t.month as int)

    where p.product_key is not null
      and t.time_key is not null

),

enriched as (

    select
        cur.time_key,
        cur.product_key,

        cur.year,
        cur.quarter,
        cur.month,

        cur.trade_value,
        cur.quantity,

        cur.value_unit,
        cur.quantity_unit,

        prev_month.trade_value as trade_value_pre_month,
        prev_month.quantity as quantity_pre_month,

        prev_year.trade_value as trade_value_pre_year,
        prev_year.quantity as quantity_pre_year,

        cur.ingest_at

    from joined cur

    left join joined prev_year
        on cur.product_key = prev_year.product_key
       and cur.year = prev_year.year + 1
       and cur.month = prev_year.month

    left join joined prev_month
        on cur.product_key = prev_month.product_key
       and (
            (
                cur.year = prev_month.year
                and cur.month = prev_month.month + 1
            )
            or
            (
                cur.year = prev_month.year + 1
                and cur.month = 1
                and prev_month.month = 12
            )
       )

),

final as (

    select
        {{ sk(['time_key','product_key']) }} as fact_trade_key,

        time_key,
        product_key,

        cast(round(trade_value, 3) as decimal(38,3)) as trade_value,
        cast(round(quantity, 3) as decimal(38,3)) as quantity,

        value_unit,
        quantity_unit,

        cast(round(trade_value_pre_month, 3) as decimal(38,3)) as trade_value_pre_month,
        cast(round(trade_value_pre_year, 3) as decimal(38,3)) as trade_value_pre_year,

        cast(round(quantity_pre_month, 3) as decimal(38,3)) as quantity_pre_month,
        cast(round(quantity_pre_year, 3) as decimal(38,3)) as quantity_pre_year,

        {{ sk(['time_key','product_key']) }} as load_id,
        current_timestamp() as created_at,
        ingest_at

    from enriched

)

select *
from final