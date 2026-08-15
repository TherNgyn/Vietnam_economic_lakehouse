{{ config(
    materialized='delta_table'
) }}

with terms as (
    select distinct
        cast(term_name as string) as term_name
    from {{ ref('stg_interest_rate') }}
    where term_name is not null
),

parsed as (
    select
        term_name,
        case
            when lower(term_name) like '%overnight%'
              or lower(term_name) like '%qua đêm%'
                then 0

            when lower(term_name) like '%12m%'
              or lower(term_name) like '%12 months%'
              or lower(term_name) like '%12 tháng%'
                then 12

            when lower(term_name) like '%9m%'
              or lower(term_name) like '%9 months%'
              or lower(term_name) like '%9 tháng%'
                then 9

            when lower(term_name) like '%6m%'
              or lower(term_name) like '%6 months%'
              or lower(term_name) like '%6 tháng%'
                then 6

            when lower(term_name) like '%3m%'
              or lower(term_name) like '%3 months%'
              or lower(term_name) like '%3 tháng%'
                then 3

            when lower(term_name) like '%2m%'
              or lower(term_name) like '%2 months%'
              or lower(term_name) like '%2 tháng%'
                then 2

            when lower(term_name) like '%1m%'
              or lower(term_name) like '%1 month%'
              or lower(term_name) like '%1 tháng%'
                then 1

            else null
        end as term_months
    from terms
)

select
    row_number() over (order by term_name) as term_key,
    term_name,
    term_months,
    current_timestamp() as created_at
from parsed