{{ config(
    materialized='delta_table'
)}}

with date_range as (
    select explode(
        sequence(
            date '1990-01-01',
            make_date(year(current_date()), 12, 31),
            interval 1 day
        )
    ) as full_date
),

final as (
    select
        cast(date_format(full_date, 'yyyyMMdd') as int)                                  as time_key,
        full_date,
        day(full_date)                                                                    as day,
        month(full_date)                                                                  as month,
        quarter(full_date)                                                                as quarter,
        year(full_date)                                                                   as year,
        cast(date_format(full_date, 'yyyyMM') as int)                                    as year_month,
        concat(cast(year(full_date) as string), 'Q', cast(quarter(full_date) as string)) as year_quarter,
        case when full_date = last_day(full_date) then true else false end                as is_month_end,
        case
            when month(full_date) in (3, 6, 9, 12)
             and full_date = last_day(full_date)
            then true else false
        end                                                                               as is_quarter_end
    from date_range
)

select *
from final