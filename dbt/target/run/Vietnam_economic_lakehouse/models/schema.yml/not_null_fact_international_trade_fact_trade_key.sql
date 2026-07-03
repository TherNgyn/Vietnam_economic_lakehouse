
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_trade_key
from gold_gold.fact_international_trade
where fact_trade_key is null



  
  
      
    ) dbt_internal_test