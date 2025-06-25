-- 


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
    from `peters-datasets`.`dbt_svop_mock_schema_presentation`.`vw_dim_customer` cte
)

select *
from cte_customer