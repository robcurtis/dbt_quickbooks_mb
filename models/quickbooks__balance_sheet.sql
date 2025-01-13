with general_ledger_by_period as (

    select *
    from {{ ref('quickbooks__general_ledger_by_period') }}
    where financial_statement_helper = 'balance_sheet'
), 

inactive_ar_accounts as (
    select distinct 
        account_id,
        source_relation
    from (
        select 
            account_id,
            source_relation,
            period_first_day,
            period_ending_balance,
            account_sub_type,
            sum(case when period_ending_balance = 0 then 1 else 0 end) 
                over (partition by account_id, source_relation order by period_first_day rows between 2 preceding and current row) as consecutive_zero_months
        from general_ledger_by_period
        where period_first_day >= dateadd(month, -12, current_date)
        and account_sub_type = 'AccountsReceivable'
    ) zero_balance_check
    where consecutive_zero_months = 3
),

final as (
    select
        period_first_day as calendar_date, --  Slated to be deprecated; we recommend using `period_first_day` or `period_last_day`
        period_first_day,
        period_last_day,
        source_relation,
        account_class,
        class_id,
        is_sub_account,
        parent_account_number,
        parent_account_name,
        account_type,
        account_sub_type,
        account_number,
        account_id,
        account_name,
        period_ending_balance as amount,
        period_ending_converted_balance as converted_amount,
        account_ordinal
    from general_ledger_by_period
    where not exists (
        select 1 
        from inactive_ar_accounts 
        where inactive_ar_accounts.account_id = general_ledger_by_period.account_id
        and inactive_ar_accounts.source_relation = general_ledger_by_period.source_relation
    )
)

select *
from final