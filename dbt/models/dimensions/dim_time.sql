{{ config(
    materialized='delta_table'
)}}


with dates as (
    select distinct date_str as report_date from {{ ref('stg_macro_indicator') }}
    union
    select distinct date_str as report_date from {{ ref('stg_ohlc') }}
    union
    select distinct date_str as report_date from {{ ref('stg_interest_rate') }}
    union
    select distinct report_date from {{ ref('stg_product_market') }}
    union
    select distinct report_date from {{ ref('stg_agriculture') }}
    union
    select distinct report_date from {{ ref('stg_gdp') }}
    union
    select distinct report_date from {{ ref('stg_investment') }}
    union
    select distinct report_date from {{ ref('stg_social_investment') }}
),

final as (
    select
        cast(date_format(to_date(report_date), 'yyyyMMdd') as int)                             as time_key,
        to_date(report_date)                                                                    as full_date,
        day(to_date(report_date))                                                               as day,
        month(to_date(report_date))                                                             as month,
        quarter(to_date(report_date))                                                           as quarter,
        year(to_date(report_date))                                                              as year,
        cast(date_format(to_date(report_date), 'yyyyMM') as int)                               as year_month,
        concat(cast(year(to_date(report_date)) as string), 'Q', cast(quarter(to_date(report_date)) as string)) as year_quarter,
        case when to_date(report_date) = last_day(to_date(report_date)) then true else false end as is_month_end,
        case
            when month(to_date(report_date)) in (3, 6, 9, 12)
             and to_date(report_date) = last_day(to_date(report_date))
            then true else false
        end as is_quarter_end
    from dates
    where report_date is not null
)

select *
from final