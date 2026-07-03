
    
    

select
    fact_social_total_investment_key as unique_field,
    count(*) as n_records

from gold_gold.fact_social_total_investment
where fact_social_total_investment_key is not null
group by fact_social_total_investment_key
having count(*) > 1


