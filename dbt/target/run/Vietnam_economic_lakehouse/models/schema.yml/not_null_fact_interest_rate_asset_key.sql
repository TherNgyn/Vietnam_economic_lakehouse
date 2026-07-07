
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select asset_key
from gold_gold.fact_interest_rate
where asset_key is null



  
  
      
    ) dbt_internal_test