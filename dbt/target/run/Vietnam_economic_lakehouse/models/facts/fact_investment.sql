
    insert into table gold_gold.fact_investment
    select `fact_gdp_key`, `time_key`, `sub_sector_key`, `unit_key`, `source_key`, `market_value`, `constant_value`, `load_id`, `created_at` from fact_investment__dbt_tmp

