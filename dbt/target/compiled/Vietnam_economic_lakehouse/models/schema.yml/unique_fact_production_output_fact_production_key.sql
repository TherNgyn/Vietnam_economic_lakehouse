
    
    

select
    fact_production_key as unique_field,
    count(*) as n_records

from gold_gold.fact_production_output
where fact_production_key is not null
group by fact_production_key
having count(*) > 1


