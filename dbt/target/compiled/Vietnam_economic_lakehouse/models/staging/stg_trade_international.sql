

select
    cast(
        concat(
            cast(`year` as string),
            '-',
            lpad(cast(`month` as string), 2, '0'),
            '-01'
        ) as date
    ) as report_date,

    cast(`year` as int) as year,
    cast(`quarter` as int) as quarter,
    cast(`month` as int) as month,

    trim(cast(product_name as string)) as product_name,
    trim(cast(`type` as string)) as product_type,

    cast(value as float) as trade_value,
    cast(quantity as float) as quantity,

    trim(cast(unit as string)) as value_unit,
    trim(cast(quantity_unit as string)) as quantity_unit,

    cast(ingest_at as timestamp) as ingest_at

from silver.international_ecommerce

where `year` is not null
  and `month` between 1 and 12
  and product_name is not null
  and `type` is not null