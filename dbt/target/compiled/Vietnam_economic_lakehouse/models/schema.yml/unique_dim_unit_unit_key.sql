
    
    

select
    unit_key as unique_field,
    count(*) as n_records

from gold_gold.dim_unit
where unit_key is not null
group by unit_key
having count(*) > 1


