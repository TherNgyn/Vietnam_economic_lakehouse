
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select value
from gold_gold.fact_production_output
where value is null



  
  
      
    ) dbt_internal_test