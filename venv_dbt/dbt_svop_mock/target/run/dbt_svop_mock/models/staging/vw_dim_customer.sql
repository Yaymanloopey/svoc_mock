

  create or replace view `peters-datasets`.`dbt_svop_mock_schema_staging`.`vw_dim_customer`
  OPTIONS()
  as 

with cte_customer as (
    select
        customer_id
        , min(event_date) as first_seen_date
        , max(event_date) as last_seen_date 
        , null customer_first_name
        , null customer_last_name
    from `peters-datasets`.`dbt_svop_mock_schema_landing`.`policy_events`
    group by 1
)
, cte_customer_latest_location as(
    select
        *
    from(
        select  
            customer_id
            , state last_seen_state
            , row_number() over (partition by customer_id order by event_date desc) as instance
        from `peters-datasets`.`dbt_svop_mock_schema_landing`.`policy_events`
    )
    where instance = 1 
)

select
    cte_customer.customer_id
    , cte_customer.customer_first_name
    , cte_customer.customer_last_name
    , cte_customer.first_seen_date
    , cte_customer.last_seen_date
    , cte_customer_latest_location.last_seen_state
from cte_customer
left join cte_customer_latest_location 
    on cte_customer.customer_id = cte_customer_latest_location.customer_id;

