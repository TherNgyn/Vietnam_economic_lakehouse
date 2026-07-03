
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select fact_ohlc_key
from gold_gold.fact_ohlc
where fact_ohlc_key is null



  
  
      
    ) dbt_internal_test