
    
    

with child as (
    select product_category_key as from_field
    from gold_gold.fact_production_output
    where product_category_key is not null
),

parent as (
    select product_category_key as to_field
    from gold_gold.dim_product_category
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


