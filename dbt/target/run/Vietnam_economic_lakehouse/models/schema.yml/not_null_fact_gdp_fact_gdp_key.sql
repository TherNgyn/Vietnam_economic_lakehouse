
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_gdp_key
from gold_gold.fact_gdp
where fact_gdp_key is null



  
  
      
    ) dbt_internal_test