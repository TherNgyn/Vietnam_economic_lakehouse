
        CREATE TABLE gold_gold.dim_term
        USING DELTA
        AS
        


with terms as (
    select distinct term_name
    from gold_staging.stg_interest_rate
    where term_name is not null
),

parsed as (
    select
        term_name,
        case
            when lower(term_name) like '%overnight%' then 0
            when lower(term_name) like '%1m%' or lower(term_name) like '%1 tháng%' then 1
            when lower(term_name) like '%3m%' or lower(term_name) like '%3 tháng%' then 3
            when lower(term_name) like '%6m%' or lower(term_name) like '%6 tháng%' then 6
            when lower(term_name) like '%12m%' or lower(term_name) like '%12 tháng%' then 12
            else null
        end as term_months
    from terms
)

select
    
    abs(xxhash64(coalesce(cast(term_name as string), '__null__')))
 as term_key,
    term_name,
    term_months
from parsed
    