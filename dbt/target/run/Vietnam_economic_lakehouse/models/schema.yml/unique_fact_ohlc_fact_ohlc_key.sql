
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    fact_ohlc_key as unique_field,
    count(*) as n_records

from gold_gold.fact_ohlc
where fact_ohlc_key is not null
group by fact_ohlc_key
having count(*) > 1



  
  
      
    ) dbt_internal_test