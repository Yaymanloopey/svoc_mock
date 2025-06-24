-- 1. How many policies were quoted but never bound?
  -- ALL POLICIES WITH NO BINDS
  SELECT
    count(distinct policy_id)
  from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
  HAVING sum(is_first_quote) > 0
  and sum(is_first_bind) = 0;

  -- ALL POLICIES WHERE A QUOTE HAPPENED, BUT THERE WAS NO FOLLOWING BIND
  with cte_first_quote as(
    select
      policy_id
      , event_date
    from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
    where is_first_quote = 1
  )
  , cte_first_bind as(
    select
      policy_id
      , event_date
    from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
    where is_first_bind = 1
  )
  select
    count(distinct fq.policy_id) distinct_policy_quote_count
    /*
      when 
    */
    , count(case when fb.event_date > fq.event_date then 1 end) bind_after_quote_count
    , count(distinct fq.policy_id) - count(case when fb.event_date > fq.event_date then 1 end) no_bind_after_quote_count
  from cte_first_quote fq
  left join cte_first_bind fb
    on fq.policy_id = fb.policy_id;




-- 2. What is the average premium for policies that were bound by channel?
  /*
    getting a list of policies that have been bound
  */
  with cte_bound_list as(
    select distinct
      policy_id
    from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
    where is_first_bind = 1
  )
  select  
    channel
    , round(avg(premium_amount),2) average_premium_amount
  from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies fcp
  where exists(
    select 1
    from cte_bound_list bl
    where bl.policy_id = fcp.policy_id
  )
  group by 1;



-- 3. Which states have the highest number of policy cancellations?
  SELECT
    state
    , count(case when lower(event_type) = 'cancel' then 1 end) cancellation_count
  from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
  group by 1
  order by count(case when lower(event_type) = 'cancel' then 1 end) desc;



-- 4. What is the average time (in days) from Quote to Bind per state?
    select
      state
      , round(avg(quote_to_bind_days),2) avg_quote_to_bind_days
    from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
    group by 1
    order by  avg(quote_to_bind_days) desc;



-- 5. Identify policies where Cancel occurred without a preceding Bind.
with cte_first_cancel as(
    select
      policy_id
      , event_date
    from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
    where is_first_cancel = 1
  )
  , cte_first_bind as(
    select
      policy_id
      , event_date
    from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
    where is_first_bind = 1
  )
  select
    fc.policy_id
    -- , count(distinct fc.policy_id) distinct_policy_cancel_count
    -- , count(case when fc.event_date > fb.event_date then 1 end) cancel_after_bind
    -- , count(distinct fc.policy_id) - count(case when fc.event_date > fb.event_date then 1 end) no_cancel_after_bind
  from cte_first_cancel fc
  left join cte_first_bind fb
    on fb.policy_id = fc.policy_id
  /*
    where cancellation happened before a bind
    OR when there is no bind at all
  */
  where fc.event_date < fb.event_date
  or fb.event_date is null
  -- group by 1
  ;



-- 6. What is the distribution of premium amounts across different sales channels?
  select
    channel
    , round(sum(premium_amount),2) premium_amount
  from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies cp
  group by 1
  order by 2 desc;



-- 7. How many policies had multiple quotes before being bound?

with cte_min_bind as(
  select
    policy_id
    , event_date
  from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
  where is_first_bind = 1
)
, cte_before_bind as(
  select 
    fcp.policy_id
  FROM peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies fcp
  left join cte_min_bind cmb
    on cmb.policy_id = fcp.policy_id
  where lower(fcp.event_type) = 'quote'
  and fcp.event_date < cmb.event_date
)

SELECT count(distinct policy_id) policies_with_multiple_quotes_before_bind
FROM cte_before_bind

-- EVENTS OVER TIME
select
  event_date
  , count(case when event_type = 'Quote' then 1 end) quote_count
  , count(case when event_type = 'Cancel' then 1 end) cancel_count
  , count(case when event_type = 'Bind' then 1 end) bind_count
from peters-datasets.dbt_svop_mock_schema_presentation.fact_customer_policies
group by 1
order by 1
;
