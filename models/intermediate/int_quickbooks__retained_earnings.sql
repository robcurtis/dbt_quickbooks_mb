with general_ledger_balances as (

    select *
    from {{ ref('int_quickbooks__general_ledger_balances') }}
),

net_income_loss as (

    select
        period_first_day,
        date_year,
        source_relation,
        sum(case when account_class = 'Revenue' then period_net_change else 0 end) as revenue_net_change,
        sum(case when account_class = 'Revenue' then period_net_converted_change else 0 end) as revenue_net_converted_change,
        sum(case when account_class = 'Expense' then period_net_change else 0 end) as expense_net_change,
        sum(case when account_class = 'Expense' then period_net_converted_change else 0 end) as expense_net_converted_change
    from general_ledger_balances
    {{ dbt_utils.group_by(3) }} 
),

retained_earnings_starter as (

    select
        cast('9999' as {{ dbt.type_string() }}) as account_id,
        source_relation,
        cast('9999-00' as {{ dbt.type_string() }}) as account_number,
        cast('Net Income Adjustment' as {{ dbt.type_string() }}) as account_name,
        false as is_sub_account,
        cast(null as {{ dbt.type_string() }}) as parent_account_number,
        cast(null as {{ dbt.type_string() }}) as parent_account_name,
        cast('Equity' as {{ dbt.type_string() }}) as account_type,
        cast('RetainedEarnings' as {{ dbt.type_string() }}) as account_sub_type,
        cast('Equity' as {{ dbt.type_string() }}) as account_class,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast('balance_sheet' as {{ dbt.type_string() }}) as financial_statement_helper,
        date_year,
        period_first_day,
        (date_trunc('month', period_first_day) + interval '1 month' - interval '1 day') as period_last_day,
        (revenue_net_change - expense_net_change) as period_net_change,
        cast(0 as {{ dbt.type_numeric() }}) as period_beginning_balance,
        cast(0 as {{ dbt.type_numeric() }}) as period_ending_balance,
        (revenue_net_converted_change - expense_net_converted_change) as period_net_converted_change,
        cast(0 as {{ dbt.type_numeric() }}) as period_beginning_converted_balance,
        cast(0 as {{ dbt.type_numeric() }}) as period_ending_converted_balance
    from net_income_loss
),

final as (

    select
        account_id,
        source_relation,
        account_number,
        account_name,
        is_sub_account,
        parent_account_number,
        parent_account_name,
        account_type,
        account_sub_type,
        account_class,
        class_id,
        financial_statement_helper,
        date_year,
        period_first_day,
        period_last_day,
        period_net_change,
        sum(period_net_change) over (
            partition by source_relation 
            order by date_year
            rows between unbounded preceding and 1 preceding
        ) as period_beginning_balance,
        sum(period_net_change) over (
            partition by source_relation 
            order by date_year
            rows between unbounded preceding and 1 preceding
        ) + 
        sum(period_net_change) over (
            partition by source_relation, date_year 
            order by period_first_day
            rows between unbounded preceding and current row
        ) as period_ending_balance,
        period_net_converted_change,
        sum(period_net_converted_change) over (
            partition by source_relation 
            order by date_year
            rows between unbounded preceding and 1 preceding
        ) as period_beginning_converted_balance,
        sum(period_net_converted_change) over (
            partition by source_relation 
            order by date_year
            rows between unbounded preceding and 1 preceding
        ) + 
        sum(period_net_converted_change) over (
            partition by source_relation, date_year 
            order by period_first_day
            rows between unbounded preceding and current row
        ) as period_ending_converted_balance
    from retained_earnings_starter
)

select *
from final
