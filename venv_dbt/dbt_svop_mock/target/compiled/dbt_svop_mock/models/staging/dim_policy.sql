

with cte_policy as (
    select
        policy_id
        , first_event_date
        , last_event_date 
    from `peters-datasets`.`dbt_svop_mock_schema_staging`.`vw_dim_policy`
)

select *
from cte_policy