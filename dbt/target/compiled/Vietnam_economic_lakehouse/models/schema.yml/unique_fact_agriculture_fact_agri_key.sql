
    
    

select
    fact_agri_key as unique_field,
    count(*) as n_records

from gold_gold.fact_agriculture
where fact_agri_key is not null
group by fact_agri_key
having count(*) > 1


