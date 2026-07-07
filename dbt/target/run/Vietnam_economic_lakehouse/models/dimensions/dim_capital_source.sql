
        CREATE TABLE gold_gold.dim_capital_source
        USING DELTA
        AS
        

select
    row_number() over (order by capital_source_name) as capital_source_key,
    capital_source_name as source_name
from (
    select distinct trim(capital_source_name) as capital_source_name
    from gold_staging.stg_social_total_investment
    where capital_source_name is not null
)
    