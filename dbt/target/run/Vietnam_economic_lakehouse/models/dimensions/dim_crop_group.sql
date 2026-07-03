
        CREATE TABLE gold_gold.dim_crop_group
        USING DELTA
        AS
        


select 1 as crop_group_key, 'ANNUAL' as crop_group_name
union all select 2, 'STAPLE'
union all select 3, 'PERENNIAL'
    