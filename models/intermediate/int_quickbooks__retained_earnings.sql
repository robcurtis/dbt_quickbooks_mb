with general_ledger_balances as (
    select *
    from {{ ref('int_quickbooks__general_ledger_balances') }}
),

net_income_loss as (
    select
        period_first_day,
        source_relation,
        sum(case when account_class = 'Revenue' then period_net_change else 0 end) as revenue_net_change,
        sum(case when account_class = 'Revenue' then period_net_converted_change else 0 end) as revenue_net_converted_change,
        sum(case when account_class = 'Expense' then period_net_change else 0 end) as expense_net_change,
        sum(case when account_class = 'Expense' then period_net_converted_change else 0 end) as expense_net_converted_change
    from general_ledger_balances
    group by period_first_day, source_relation
),

manual_retained_earnings as (
    select
        period_first_day,
        source_relation,
        sum(period_net_change) as manual_re_change,
        sum(period_net_converted_change) as manual_re_converted_change
    from general_ledger_balances
    where account_sub_type = 'RetainedEarnings'
    group by period_first_day, source_relation
),

retained_earnings_starter as (
    select
        cast('9999' as {{ dbt.type_string() }}) as account_id,
        nil.source_relation,
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
        cast({{ dbt.date_trunc("year", "nil.period_first_day") }} as date) as date_year,
        nil.period_first_day,
        cast({{ dbt.last_day("nil.period_first_day", "month") }} as date) as period_last_day,
        coalesce(revenue_net_change, 0) - coalesce(expense_net_change, 0) as period_net_change,
        coalesce(revenue_net_converted_change, 0) - coalesce(expense_net_converted_change, 0) as period_net_converted_change
    from net_income_loss nil
    left join manual_retained_earnings mre
        on nil.period_first_day = mre.period_first_day
        and nil.source_relation = mre.source_relation
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
        case when extract(month from period_first_day) = 1 then 0
             else sum(period_net_change) over (
                partition by source_relation, date_year
                order by period_first_day
                rows between unbounded preceding and 1 preceding
             )
        end as period_beginning_balance,
        sum(period_net_change) over (
            partition by source_relation, date_year
            order by period_first_day
            rows between unbounded preceding and current row
        ) as period_ending_balance,
        period_net_converted_change,
        case when extract(month from period_first_day) = 1 then 0
             else sum(period_net_converted_change) over (
                partition by source_relation, date_year
                order by period_first_day
                rows between unbounded preceding and 1 preceding
             )
        end as period_beginning_converted_balance,
        sum(period_net_converted_change) over (
            partition by source_relation, date_year
            order by period_first_day
            rows between unbounded preceding and current row
        ) as period_ending_converted_balance
    from retained_earnings_starter
)

select *
from final