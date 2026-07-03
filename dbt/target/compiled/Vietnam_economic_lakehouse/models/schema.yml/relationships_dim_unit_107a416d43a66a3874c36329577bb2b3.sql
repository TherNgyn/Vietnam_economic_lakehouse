
    
    

with child as (
    select unit_type_key as from_field
    from gold_gold.dim_unit
    where unit_type_key is not null
),

parent as (
    select unit_type_key as to_field
    from gold_gold.dim_unit_type
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


