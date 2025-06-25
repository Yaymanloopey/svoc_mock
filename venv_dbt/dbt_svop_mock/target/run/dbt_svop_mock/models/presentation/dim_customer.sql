-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `peters-datasets`.`dbt_svop_mock_schema_presentation`.`dim_customer` as DBT_INTERNAL_DEST
        using (-- 


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
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.sys_record_checksum = DBT_INTERNAL_DEST.sys_record_checksum))

    
    when matched then update set
        `customer_id` = DBT_INTERNAL_SOURCE.`customer_id`,`customer_first_name` = DBT_INTERNAL_SOURCE.`customer_first_name`,`customer_last_name` = DBT_INTERNAL_SOURCE.`customer_last_name`,`first_seen_date` = DBT_INTERNAL_SOURCE.`first_seen_date`,`last_seen_date` = DBT_INTERNAL_SOURCE.`last_seen_date`,`last_seen_state` = DBT_INTERNAL_SOURCE.`last_seen_state`,`sys_record_checksum` = DBT_INTERNAL_SOURCE.`sys_record_checksum`,`sys_insert_datetime` = DBT_INTERNAL_SOURCE.`sys_insert_datetime`
    

    when not matched then insert
        (`customer_id`, `customer_first_name`, `customer_last_name`, `first_seen_date`, `last_seen_date`, `last_seen_state`, `sys_record_checksum`, `sys_insert_datetime`)
    values
        (`customer_id`, `customer_first_name`, `customer_last_name`, `first_seen_date`, `last_seen_date`, `last_seen_state`, `sys_record_checksum`, `sys_insert_datetime`)


    