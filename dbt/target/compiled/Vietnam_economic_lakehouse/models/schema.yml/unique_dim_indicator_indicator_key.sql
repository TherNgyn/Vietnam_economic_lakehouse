
    
    

select
    indicator_key as unique_field,
    count(*) as n_records

from gold_gold.dim_indicator
where indicator_key is not null
group by indicator_key
having count(*) > 1


