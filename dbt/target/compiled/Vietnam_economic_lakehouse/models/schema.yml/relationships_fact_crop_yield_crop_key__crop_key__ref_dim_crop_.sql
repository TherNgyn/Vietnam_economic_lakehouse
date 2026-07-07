
    
    

with child as (
    select crop_key as from_field
    from gold_gold.fact_crop_yield
    where crop_key is not null
),

parent as (
    select crop_key as to_field
    from gold_gold.dim_crop
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


