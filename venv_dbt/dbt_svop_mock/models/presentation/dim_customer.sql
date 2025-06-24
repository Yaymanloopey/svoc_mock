-- {{ config(materialized='table') }}
{{ config(
    materialized='incremental',
    unique_key='sys_record_checksum'
) }}

with cte_customer as (
    select
        -- dim_customer_sk
        customer_id
        , customer_first_name
        , customer_last_name
        , first_seen_date
        , last_seen_date
        , last_seen_state
        , sys_record_checksum
        , sys_insert_datetime
    from {{ ref('vw_dim_customer') }} cte
)

select *
from cte_customer