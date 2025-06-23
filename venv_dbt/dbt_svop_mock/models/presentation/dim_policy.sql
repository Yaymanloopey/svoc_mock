{{ config(materialized='table') }}
-- {{ config(materialized='incremental') }}
with cte_policy as (
    select
        -- dim_policy_sk
        policy_id
        , first_event_date
        , last_event_date 
    from {{ ref('vw_dim_policy') }}
)

select 
    *
from cte_policy
