{{ config(materialized='table') }}
-- {{ config(materialized='incremental') }}

with cte_customer as (
    select
        -- dim_customer_sk
        customer_id
        , customer_first_name
        , customer_last_name
        , first_seen_date
        , last_seen_date
        , last_seen_state
    from {{ ref('vw_dim_customer') }}
)

select *
from cte_customer

