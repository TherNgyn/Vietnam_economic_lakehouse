
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unit_key
from gold_gold.fact_total_investment
where unit_key is null



  
  
      
    ) dbt_internal_test