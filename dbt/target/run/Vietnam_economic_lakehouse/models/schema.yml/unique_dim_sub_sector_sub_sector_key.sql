
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    sub_sector_key as unique_field,
    count(*) as n_records

from gold_gold.dim_sub_sector
where sub_sector_key is not null
group by sub_sector_key
having count(*) > 1



  
  
      
    ) dbt_internal_test