with first_activity as (
    select
        account_id,
        source_relation,
        min(cast(date_trunc('month', transaction_date) as date)) as first_activity_date
    from {{ ref('int_quickbooks__general_ledger') }}
    where adjusted_amount != 0
    group by account_id, source_relation
),

monthly_activity as (
    select
        gl.account_id,
        gl.source_relation,
        cast(date_trunc('month', gl.transaction_date) as date) as period_first_day,
        sum(gl.adjusted_amount) as period_net_change
    from {{ ref('int_quickbooks__general_ledger') }} gl
    group by 
        gl.account_id,
        gl.source_relation,
        cast(date_trunc('month', gl.transaction_date) as date)
),

inactive_check as (
    select
        ma.*,
        sum(case when period_net_change = 0 then 1 else 0 end)
            over (partition by ma.account_id, ma.source_relation order by period_first_day rows between 2 preceding and current row) as consecutive_zero_months,
        lag(period_first_day, 3) over (partition by ma.account_id, ma.source_relation order by period_first_day) as start_of_inactive_period
    from monthly_activity ma
    inner join first_activity fa
        on ma.account_id = fa.account_id
        and ma.source_relation = fa.source_relation
        and ma.period_first_day >= fa.first_activity_date
),

inactive_dates as (
    select
        account_id,
        source_relation,
        min(case when consecutive_zero_months = 3 then start_of_inactive_period end) as first_inactive_date,
        max(case when period_net_change != 0 then period_first_day end) as last_active_date
    from inactive_check
    group by account_id, source_relation
)

select *
from inactive_dates