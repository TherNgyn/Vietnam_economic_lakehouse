
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    investment_by_sector_key as unique_field,
    count(*) as n_records

from gold_gold.fact_investment_by_sector
where investment_by_sector_key is not null
group by investment_by_sector_key
having count(*) > 1



  
  
      
    ) dbt_internal_test