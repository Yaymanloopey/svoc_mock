
    
    

with dbt_test__target as (

  select policy_id as unique_field
  from `peters-datasets`.`dbt_svop_mock_schema_presentation`.`vw_dim_policy`
  where policy_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


