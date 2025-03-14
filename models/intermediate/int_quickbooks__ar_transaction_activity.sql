with ar_accounts as (
    select 
        account_id,
        source_relation,
        account_number,
        account_name,
        is_sub_account,
        parent_account_number,
        parent_account_name
    from {{ ref('stg_quickbooks__account') }}
    where account_sub_type = 'AccountsReceivable'
),

transaction_activity as (
    select
        gl.account_id,
        gl.source_relation,
        cast(date_trunc('month', gl.transaction_date) as date) as period_first_day,
        sum(gl.adjusted_amount) as period_net_change
    from {{ ref('int_quickbooks__general_ledger') }} gl
    inner join ar_accounts a 
        on gl.account_id = a.account_id 
        and gl.source_relation = a.source_relation
    group by 1, 2, 3
),

first_activity as (
    select
        account_id,
        source_relation,
        min(period_first_day) as first_activity_date
    from transaction_activity
    where period_net_change != 0
    group by 1, 2
),

monthly_activity as (
    select
        ta.*,
        fa.first_activity_date
    from transaction_activity ta
    inner join first_activity fa
        on ta.account_id = fa.account_id
        and ta.source_relation = fa.source_relation
)

select *
from monthly_activity