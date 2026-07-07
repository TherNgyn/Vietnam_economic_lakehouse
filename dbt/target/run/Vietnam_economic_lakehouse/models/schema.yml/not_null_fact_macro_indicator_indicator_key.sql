
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select indicator_key
from gold_gold.fact_macro_indicator
where indicator_key is null



  
  
      
    ) dbt_internal_test