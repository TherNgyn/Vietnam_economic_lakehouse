
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_interest_key
from gold_gold.fact_interest_rate
where fact_interest_key is null



  
  
      
    ) dbt_internal_test