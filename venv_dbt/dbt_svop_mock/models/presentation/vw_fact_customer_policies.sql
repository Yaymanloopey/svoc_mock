with cte_customer_policies as (
    select
        policy_id
        , event_type
        , event_date
        , customer_id
        , channel
        , premium_amount
        , state
        /*
            creating sys_attributes
        */
        , to_hex(md5(concat(
            COALESCE(CAST(CTE.policy_id AS STRING),''),'||',
            COALESCE(CAST(CTE.event_type AS STRING),''),'||',
            COALESCE(CAST(CTE.event_date AS STRING),''),'||',
            COALESCE(CAST(CTE.customer_id AS STRING),''),'||',
            COALESCE(CAST(CTE.channel AS STRING),''),'||',
            COALESCE(CAST(CTE.premium_amount AS STRING),''),'||',
            COALESCE(CAST(CTE.state AS STRING),''),'||'
        ))) sys_record_checksum
        , current_datetime('Australia/Sydney') sys_insert_datetime
    from {{ ref('policy_events') }} cte
)
/*
    for each policy, identifying the minimum date for each event
    row_number() logic can be used, however, as data scales, this function can become costly to process
*/
, cte_first_event_date as(
    select
        policy_id
        /*
            Calculating first instance of each event
        */
        , min(case when lower(event_type) = 'quote' then event_date end) first_quote_date
        , min(case when lower(event_type) = 'bind' then event_date end) first_bind_date
        , min(case when lower(event_type) = 'cancel' then event_date end) first_cancel_date
        /*
            determining time between specified events
        */
        , date_diff(min(case when lower(event_type) = 'bind' then event_date end), min(case when lower(event_type) = 'quote' then event_date end), DAY) quote_to_bind_days
        , date_diff(min(case when lower(event_type) = 'cancel' then event_date end), min(case when lower(event_type) = 'bind' then event_date end), DAY) bind_to_cancel_days
    from {{ ref('policy_events') }}
    group by 1
)

select distinct
    -- fact_customer_policies_sk
    cp.policy_id
    , cp.event_type
    , cp.event_date
    , cp.customer_id
    , cp.channel
    , cp.premium_amount
    , cp.state
    /*
        flagging first instance of each event
    */
    , case when fed_q.first_quote_date is not null then 1 else 0 end as is_first_quote
    , case when fed_b.first_bind_date is not null then 1 else 0 end as is_first_bind
    , case when fed_c.first_cancel_date is not null then 1 else 0 end as is_first_cancel
    /*
        not considering below cases, as this dirty data will skew the results:
        - binds happen before quotes
        - cancels happen before binds 
    */
    , case when fed.quote_to_bind_days < 0 then 0 else fed.quote_to_bind_days end quote_to_bind_days
    , case when fed.bind_to_cancel_days < 0 then 0 else fed.bind_to_cancel_days end bind_to_cancel_days
    /*
        general calculation of time between sequential events
    */
    , DATE_DIFF(LEAD(event_date) over (partition by cp.policy_id order by event_date), event_date, DAY) days_until_to_next_event
    , DATE_DIFF(event_date,LAG(event_date) over (partition by cp.policy_id order by event_date), DAY) days_since_last_event
    , cp.sys_record_checksum
    , cp.sys_insert_datetime
from cte_customer_policies cp
left join cte_first_event_date fed_q
    on cp.policy_id = fed_q.policy_id
    and cp.event_date = fed_q.first_quote_date
    and lower(cp.event_type) = 'quote'
left join cte_first_event_date fed_b
    on cp.policy_id = fed_b.policy_id
    and cp.event_date = fed_b.first_bind_date
    and lower(cp.event_type) = 'bind'
left join cte_first_event_date fed_c
    on cp.policy_id = fed_c.policy_id
    and cp.event_date = fed_c.first_cancel_date
    and lower(cp.event_type) = 'cancel'
left join cte_first_event_date fed
    on cp.policy_id = fed.policy_id