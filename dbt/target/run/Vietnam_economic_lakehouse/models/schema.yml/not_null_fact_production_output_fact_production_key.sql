
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_production_key
from gold_gold.fact_production_output
where fact_production_key is null



  
  
      
    ) dbt_internal_test