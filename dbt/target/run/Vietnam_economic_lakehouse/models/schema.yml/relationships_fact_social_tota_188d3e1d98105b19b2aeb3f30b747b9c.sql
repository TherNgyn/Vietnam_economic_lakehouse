
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select source_key as from_field
    from gold_gold.fact_social_total_investment
    where source_key is not null
),

parent as (
    select source_key as to_field
    from gold_gold.dim_source
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test