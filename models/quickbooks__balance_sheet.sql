with general_ledger_by_period as (

    select *
    from {{ ref('quickbooks__general_ledger_by_period') }}
    where financial_statement_helper = 'balance_sheet'
),  

account_numbers_consolidated as (
    select
        period_first_day as calendar_date,
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
        case 
            when source_relation = 'quickbooks_bdhsc' 
            and account_number in ('1198', '1199') then '1100'
            else account_number 
        end as account_number,
        max(case 
            when source_relation = 'quickbooks_bdhsc' 
            and account_number = '1100' then account_id
            when source_relation != 'quickbooks_bdhsc' then account_id
        end) as account_id,
        max(case 
            when source_relation = 'quickbooks_bdhsc' 
            and account_number = '1100' then account_name
            when source_relation != 'quickbooks_bdhsc' then account_name
        end) as account_name,
        sum(period_ending_balance) as amount,
        sum(period_ending_converted_balance) as converted_amount,
        min(account_ordinal) as account_ordinal
    from general_ledger_by_period
    group by 1,2,3,4,5,6,7,8,9,10,11,12
)

select *
from account_numbers_consolidated