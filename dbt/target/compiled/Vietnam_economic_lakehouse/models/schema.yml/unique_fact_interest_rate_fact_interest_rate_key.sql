
    
    

select
    fact_interest_rate_key as unique_field,
    count(*) as n_records

from gold_gold.fact_interest_rate
where fact_interest_rate_key is not null
group by fact_interest_rate_key
having count(*) > 1


