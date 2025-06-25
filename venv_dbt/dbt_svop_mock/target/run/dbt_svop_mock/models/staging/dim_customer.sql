
  
    

    create or replace table `peters-datasets`.`dbt_svop_mock_schema_staging`.`dim_customer`
      
    
    

    OPTIONS()
    as (
      

with cte_customer as (
    select
        customer_id
        , customer_first_name
        , customer_last_name
        , first_seen_date
        , last_seen_date
        , last_seen_state
    from `peters-datasets`.`dbt_svop_mock_schema_staging`.`vw_dim_customer`
)

select *
from with cte_customer
    );
  