
    
    

select
    fact_key as unique_field,
    count(*) as n_records

from gold_gold.fact_macro_indicator
where fact_key is not null
group by fact_key
having count(*) > 1


