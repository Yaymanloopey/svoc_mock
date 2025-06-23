{{ config(materialized='view') }}
/*
    This view is used to create the dim_policy table
    For each unique policy_id, we want to know the first_event_date and last_event_date
*/
with cte_policy as (
    select
        policy_id
        , min(event_date) as first_event_date
        , max(event_date) as last_event_date 
    from {{ ref('policy_events') }}
    group by 1
)

select 
        -- dim_policy_sk
        policy_id
        , first_event_date
        , last_event_date 
from cte_policy cp
