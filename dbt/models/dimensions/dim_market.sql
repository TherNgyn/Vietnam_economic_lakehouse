{{ config(materialized='table') }}

with markets as (
    select distinct market_name, country from {{ ref('stg_ohlc') }}
    union
    select distinct market_name, country from {{ ref('stg_interest_rate') }}
)

select
    {{ sk(['market_name', 'country']) }} as market_key,
    market_name,
    country
from markets
where market_name is not null