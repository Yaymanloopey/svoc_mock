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
        , to_hex(md5(concat(
            COALESCE(CAST(CTE.customer_id AS STRING),''),'||'
        ))) sys_record_checksum
        , current_datetime('Australia/Sydney') sys_insert_datetime
    from {{ ref('policy_events') }} cte
    group by 1, sys_record_checksum, sys_insert_datetime
)

select 
        -- dim_policy_sk
        cp.policy_id
        , cp.first_event_date
        , cp.last_event_date 
        , cp.sys_record_checksum
        , cp.sys_insert_datetime
from cte_policy cp
