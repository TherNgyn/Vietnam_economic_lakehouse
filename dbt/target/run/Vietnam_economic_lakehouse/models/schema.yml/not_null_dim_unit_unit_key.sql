
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unit_key
from gold_gold.dim_unit
where unit_key is null



  
  
      
    ) dbt_internal_test