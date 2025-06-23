{{ config(materialized='table') }}
-- {{ config(
--     materialized='incremental'
--     , unique_key='SYS_RECORD_CHECKSUM'
-- ) }}

with cte_customer_policies as (
    select
        policy_id,
        event_type,
        event_date,
        customer_id,
        channel,
        premium_amount,
        state,
        is_first_quote,
        is_first_bind,
        is_first_cancel,
        quote_to_bind_days,
        bind_to_cancel_days,
        days_until_to_next_event,
        days_since_last_event
    from {{ ref('vw_fact_customer_policies') }} cte
)

select 
    policy_id,
        event_type,
        event_date,
        customer_id,
        channel,
        premium_amount,
        state,
        is_first_quote,
        is_first_bind,
        is_first_cancel,
        quote_to_bind_days,
        bind_to_cancel_days,
        days_until_to_next_event,
        days_since_last_event
from cte_customer_policies

