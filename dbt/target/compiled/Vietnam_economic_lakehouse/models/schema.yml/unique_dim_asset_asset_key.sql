
    
    

select
    asset_key as unique_field,
    count(*) as n_records

from gold_gold.dim_asset
where asset_key is not null
group by asset_key
having count(*) > 1


