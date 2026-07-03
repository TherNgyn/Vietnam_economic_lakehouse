
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select period_grain
from gold_gold.fact_production_output
where period_grain is null



  
  
      
    ) dbt_internal_test