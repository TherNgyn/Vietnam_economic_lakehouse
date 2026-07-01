
    insert into table gold_gold.fact_agriculture
    select `fact_agri_key`, `time_key`, `crop_key`, `source_key`, `production`, `area`, `load_id`, `created_at` from fact_agriculture__dbt_tmp

