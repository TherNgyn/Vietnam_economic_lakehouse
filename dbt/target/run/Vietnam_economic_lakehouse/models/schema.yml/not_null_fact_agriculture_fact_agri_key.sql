
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_agri_key
from gold_gold.fact_agriculture
where fact_agri_key is null



  
  
      
    ) dbt_internal_test