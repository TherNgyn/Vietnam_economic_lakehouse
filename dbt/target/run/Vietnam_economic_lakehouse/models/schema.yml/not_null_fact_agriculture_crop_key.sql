
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select crop_key
from gold_gold.fact_agriculture
where crop_key is null



  
  
      
    ) dbt_internal_test