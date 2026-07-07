
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select crop_key
from gold_gold.fact_crop_yield
where crop_key is null



  
  
      
    ) dbt_internal_test