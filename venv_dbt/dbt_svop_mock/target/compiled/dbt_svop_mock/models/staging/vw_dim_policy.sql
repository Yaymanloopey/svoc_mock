

with cte_policy as (
    select
        policy_id
        , min(event_date) as first_event_date
        , max(event_date) as last_event_date 
    from `peters-datasets`.`dbt_svop_mock_schema_landing`.`policy_events`
    group by 1
)

select *
from cte_policy