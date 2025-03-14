with inactive_check as (
    select
        account_id,
        source_relation,
        period_first_day,
        period_net_change,
        first_activity_date,
        sum(case when period_net_change = 0 then 1 else 0 end)
            over (partition by account_id, source_relation 
                  order by period_first_day 
                  rows between 2 preceding and current row) as consecutive_zero_months,
        lag(period_first_day, 3) over (
            partition by account_id, source_relation 
            order by period_first_day
        ) as start_of_inactive_period
    from {{ ref('int_quickbooks__ar_transaction_activity') }}
    where period_first_day >= first_activity_date
),

inactive_dates as (
    select
        account_id,
        source_relation,
        min(case when consecutive_zero_months = 3 
            then start_of_inactive_period end) as first_inactive_date,
        max(case when period_net_change != 0 
            then period_first_day end) as last_active_date
    from inactive_check
    group by 1, 2
)

select 
    account_id,
    source_relation,
    first_inactive_date,
    last_active_date,
    {{ dbt.current_timestamp() }} as dbt_updated_at
from inactive_dates