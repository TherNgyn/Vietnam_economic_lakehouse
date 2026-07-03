
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sub_sector_key
from gold_gold.dim_sub_sector
where sub_sector_key is null



  
  
      
    ) dbt_internal_test