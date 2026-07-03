
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sector_key
from gold_gold.fact_investment_by_sector
where sector_key is null



  
  
      
    ) dbt_internal_test