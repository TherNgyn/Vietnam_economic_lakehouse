
    
    

select
    crop_key as unique_field,
    count(*) as n_records

from gold_gold.dim_crop
where crop_key is not null
group by crop_key
having count(*) > 1


