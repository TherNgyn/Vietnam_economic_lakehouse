{{ config(materialized='table') }}

select 1 as crop_group_key, 'ANNUAL' as crop_group_name
union all select 2, 'STAPLE'
union all select 3, 'PERENNIAL'