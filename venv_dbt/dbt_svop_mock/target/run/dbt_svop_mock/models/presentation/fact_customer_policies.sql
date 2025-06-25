-- back compat for old kwarg name
  
  
        
            
	    
	    
            
        
    

    

    merge into `peters-datasets`.`dbt_svop_mock_schema_presentation`.`fact_customer_policies` as DBT_INTERNAL_DEST
        using (-- 


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
        days_since_last_event,
        sys_record_checksum,
        sys_insert_datetime
    from `peters-datasets`.`dbt_svop_mock_schema_presentation`.`vw_fact_customer_policies` cte
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
        days_since_last_event,
        sys_record_checksum,
        sys_insert_datetime
from cte_customer_policies
        ) as DBT_INTERNAL_SOURCE
        on ((DBT_INTERNAL_SOURCE.sys_record_checksum = DBT_INTERNAL_DEST.sys_record_checksum))

    
    when matched then update set
        `policy_id` = DBT_INTERNAL_SOURCE.`policy_id`,`event_type` = DBT_INTERNAL_SOURCE.`event_type`,`event_date` = DBT_INTERNAL_SOURCE.`event_date`,`customer_id` = DBT_INTERNAL_SOURCE.`customer_id`,`channel` = DBT_INTERNAL_SOURCE.`channel`,`premium_amount` = DBT_INTERNAL_SOURCE.`premium_amount`,`state` = DBT_INTERNAL_SOURCE.`state`,`is_first_quote` = DBT_INTERNAL_SOURCE.`is_first_quote`,`is_first_bind` = DBT_INTERNAL_SOURCE.`is_first_bind`,`is_first_cancel` = DBT_INTERNAL_SOURCE.`is_first_cancel`,`quote_to_bind_days` = DBT_INTERNAL_SOURCE.`quote_to_bind_days`,`bind_to_cancel_days` = DBT_INTERNAL_SOURCE.`bind_to_cancel_days`,`days_until_to_next_event` = DBT_INTERNAL_SOURCE.`days_until_to_next_event`,`days_since_last_event` = DBT_INTERNAL_SOURCE.`days_since_last_event`,`sys_record_checksum` = DBT_INTERNAL_SOURCE.`sys_record_checksum`,`sys_insert_datetime` = DBT_INTERNAL_SOURCE.`sys_insert_datetime`
    

    when not matched then insert
        (`policy_id`, `event_type`, `event_date`, `customer_id`, `channel`, `premium_amount`, `state`, `is_first_quote`, `is_first_bind`, `is_first_cancel`, `quote_to_bind_days`, `bind_to_cancel_days`, `days_until_to_next_event`, `days_since_last_event`, `sys_record_checksum`, `sys_insert_datetime`)
    values
        (`policy_id`, `event_type`, `event_date`, `customer_id`, `channel`, `premium_amount`, `state`, `is_first_quote`, `is_first_bind`, `is_first_cancel`, `quote_to_bind_days`, `bind_to_cancel_days`, `days_until_to_next_event`, `days_since_last_event`, `sys_record_checksum`, `sys_insert_datetime`)


    