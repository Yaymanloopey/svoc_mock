-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='sys_record_checksum'
) }}
with cte_policy as (
    select
        -- dim_policy_sk
        policy_id
        , first_event_date
        , last_event_date
        , sys_record_checksum
        , sys_insert_datetime
    from {{ ref('vw_dim_policy') }}
)

select 
    *
from cte_policy
