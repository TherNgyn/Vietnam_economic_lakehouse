
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select time_key
from gold_gold.fact_investment_by_sector
where time_key is null



  
  
      
    ) dbt_internal_test