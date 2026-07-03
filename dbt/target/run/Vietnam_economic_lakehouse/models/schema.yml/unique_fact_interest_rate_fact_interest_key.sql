
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    fact_interest_key as unique_field,
    count(*) as n_records

from gold_gold.fact_interest_rate
where fact_interest_key is not null
group by fact_interest_key
having count(*) > 1



  
  
      
    ) dbt_internal_test