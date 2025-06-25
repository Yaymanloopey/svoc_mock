-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `peters-datasets`.`dbt_svop_mock_schema_presentation`.`dim_policy` as DBT_INTERNAL_DEST
        using (-- 

with cte_policy as (
    select
        -- dim_policy_sk
        policy_id
        , first_event_date
        , last_event_date
        , sys_record_checksum
        , sys_insert_datetime
    from `peters-datasets`.`dbt_svop_mock_schema_presentation`.`vw_dim_policy`
)

select 
    *
from cte_policy
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.sys_record_checksum = DBT_INTERNAL_DEST.sys_record_checksum))

    
    when matched then update set
        `policy_id` = DBT_INTERNAL_SOURCE.`policy_id`,`first_event_date` = DBT_INTERNAL_SOURCE.`first_event_date`,`last_event_date` = DBT_INTERNAL_SOURCE.`last_event_date`,`sys_record_checksum` = DBT_INTERNAL_SOURCE.`sys_record_checksum`,`sys_insert_datetime` = DBT_INTERNAL_SOURCE.`sys_insert_datetime`
    

    when not matched then insert
        (`policy_id`, `first_event_date`, `last_event_date`, `sys_record_checksum`, `sys_insert_datetime`)
    values
        (`policy_id`, `first_event_date`, `last_event_date`, `sys_record_checksum`, `sys_insert_datetime`)


    