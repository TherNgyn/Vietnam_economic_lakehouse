
        CREATE TABLE gold_gold.fact_international_trade
        USING DELTA
        AS
        

with base as (
    select * from gold_staging.stg_trade_international
),

joined as (
    select
        t.time_key,
        p.product_key,
        b.year,
        b.quarter,
        b.month,
        b.trade_value,
        b.value_unit,
        b.quantity,
        b.quantity_unit,
        b.ingest_at
    from base b
    left join gold_gold.dim_product p
        on b.product_name = p.product_name
        and b.product_type = p.product_type
    left join gold_gold.dim_time t
        on b.report_date = t.full_date
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
        cur.value_unit,
        cur.quantity,
        cur.quantity_unit,
        prev_month.trade_value as trade_value_pre_month,
        prev_year.trade_value as trade_value_pre_year,
        cur.ingest_at
    from joined cur
    
    -- Cùng tháng năm trước (YoY Base Value)
    left join joined prev_year
        on cur.product_key = prev_year.product_key
       and cur.year = prev_year.year + 1
       and cur.month = prev_year.month
       
    -- Tháng liền kề trước đó (MoM Base Value)
    left join joined prev_month
        on cur.product_key = prev_month.product_key
       and (
            (cur.year = prev_month.year and cur.month = prev_month.month + 1)
            or
            (cur.year = prev_month.year + 1 and cur.month = 1 and prev_month.month = 12)
       )
)

select
    cast(time_key as int) as time_key,
    cast(product_key as int) as product_key,
    cast(year as int) as year,               -- Giữ lại làm hạt phân vùng cho Mart Window
    cast(quarter as int) as quarter,         -- Giữ lại làm hạt phân vùng cho Mart Window
    cast(month as int) as month,             -- Giữ lại làm hạt phân vùng cho Mart Window
    cast(coalesce(trade_value, 0) as float) as trade_value,
    cast(value_unit as string) as value_unit,
    cast(coalesce(quantity, 0) as float) as quantity,
    cast(quantity_unit as string) as quantity_unit,
    cast(coalesce(trade_value_pre_month, 0) as float) as trade_value_pre_month,
    cast(coalesce(trade_value_pre_year, 0) as float) as trade_value_pre_year,
    ingest_at
from enriched
    