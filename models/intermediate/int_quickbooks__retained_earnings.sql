with general_ledger_balances as (
    select *
    from {{ ref('int_quickbooks__general_ledger_balances') }}
),

-- Get the actual retained earnings account info for each source_relation
retained_earnings_accounts as (
    select distinct
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
        financial_statement_helper
    from general_ledger_balances
    where account_sub_type = 'RetainedEarnings'
),

-- Get the manual retained earnings entries
manual_retained_earnings as (
    select
        date_year,
        source_relation,
        sum(period_net_change) as manual_re_change,
        sum(period_net_converted_change) as manual_re_converted_change
    from general_ledger_balances
    where account_sub_type = 'RetainedEarnings'
    group by date_year, source_relation
),

-- Calculate net income (revenue - expenses) by year
net_income_loss_yearly as (
    select
        date_year,
        source_relation,
        sum(case when account_class = 'Revenue' then period_net_change else 0 end) -
        sum(case when account_class = 'Expense' then period_net_change else 0 end) as net_income_change,
        sum(case when account_class = 'Revenue' then period_net_converted_change else 0 end) -
        sum(case when account_class = 'Expense' then period_net_converted_change else 0 end) as net_income_converted_change
    from general_ledger_balances
    group by date_year, source_relation
),

-- Calculate cumulative retained earnings by year
retained_earnings_yearly as (
    select
        nil.date_year,
        nil.source_relation,
        nil.net_income_change,
        nil.net_income_converted_change,
        coalesce(mre.manual_re_change, 0) as manual_re_change,
        coalesce(mre.manual_re_converted_change, 0) as manual_re_converted_change,
        sum(nil.net_income_change + coalesce(mre.manual_re_change, 0)) over (
            partition by nil.source_relation
            order by nil.date_year
            rows between unbounded preceding and 1 preceding
        ) as cumulative_retained_earnings,
        sum(nil.net_income_converted_change + coalesce(mre.manual_re_converted_change, 0)) over (
            partition by nil.source_relation
            order by nil.date_year
            rows between unbounded preceding and 1 preceding
        ) as cumulative_retained_earnings_converted
    from net_income_loss_yearly nil
    left join manual_retained_earnings mre
        on nil.date_year = mre.date_year
        and nil.source_relation = mre.source_relation
),

-- Create monthly records
base_records as (
    select
        gl.source_relation,
        gl.date_year,
        gl.period_first_day,
        (date_trunc('month', gl.period_first_day) + interval '1 month' - interval '1 day')::date as period_last_day,
        rey.cumulative_retained_earnings,
        rey.cumulative_retained_earnings_converted,
        coalesce(nil.net_income_change, 0) as current_year_net_income,
        coalesce(nil.net_income_converted_change, 0) as current_year_net_income_converted
    from (
        select distinct period_first_day, date_year, source_relation
        from general_ledger_balances
    ) gl
    left join retained_earnings_yearly rey
        on gl.date_year = rey.date_year
        and gl.source_relation = rey.source_relation
    left join net_income_loss_yearly nil
        on gl.date_year = nil.date_year
        and gl.source_relation = nil.source_relation
),

-- Calculate monthly cumulative net income for the current year
monthly_net_income as (
    select
        source_relation,
        date_year,
        period_first_day,
        sum(case when account_class = 'Revenue' then period_net_change else 0 end) -
        sum(case when account_class = 'Expense' then period_net_change else 0 end) as net_income_change,
        sum(case when account_class = 'Revenue' then period_net_converted_change else 0 end) -
        sum(case when account_class = 'Expense' then period_net_converted_change else 0 end) as net_income_converted_change
    from general_ledger_balances
    group by source_relation, date_year, period_first_day
),

monthly_cumulative_net_income as (
    select
        source_relation,
        date_year,
        period_first_day,
        sum(net_income_change) over (
            partition by source_relation, date_year
            order by period_first_day
            rows between unbounded preceding and current row
        ) as cumulative_net_income_change,
        sum(net_income_converted_change) over (
            partition by source_relation, date_year
            order by period_first_day
            rows between unbounded preceding and current row
        ) as cumulative_net_income_converted_change
    from monthly_net_income
),

final as (
    -- Net Income Adjustment record (monthly cumulative)
    select
        cast('9999' as {{ dbt.type_string() }}) as account_id,
        br.source_relation,
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
        br.date_year,
        br.period_first_day,
        br.period_last_day,
        coalesce(mni.net_income_change, 0) as period_net_change,
        coalesce(mni.net_income_converted_change, 0) as period_net_converted_change,
        coalesce(mcni.cumulative_net_income_change - mni.net_income_change, 0) as period_beginning_balance,
        coalesce(mcni.cumulative_net_income_change, 0) as period_ending_balance,
        coalesce(mcni.cumulative_net_income_converted_change - mni.net_income_converted_change, 0) as period_beginning_converted_balance,
        coalesce(mcni.cumulative_net_income_converted_change, 0) as period_ending_converted_balance,
        1 as sort_order
    from base_records br
    left join monthly_net_income mni
        on br.source_relation = mni.source_relation
        and br.date_year = mni.date_year
        and br.period_first_day = mni.period_first_day
    left join monthly_cumulative_net_income mcni
        on br.source_relation = mcni.source_relation
        and br.date_year = mcni.date_year
        and br.period_first_day = mcni.period_first_day

    union all

    -- Total Retained Earnings record (annual, unchanged)
    select
        rea.account_id,
        br.source_relation,
        rea.account_number,
        rea.account_name,
        rea.is_sub_account,
        rea.parent_account_number,
        rea.parent_account_name,
        rea.account_type,
        rea.account_sub_type,
        rea.account_class,
        rea.class_id,
        rea.financial_statement_helper,
        br.date_year,
        br.period_first_day,
        br.period_last_day,
        0 as period_net_change,
        0 as period_net_converted_change,
        coalesce(br.cumulative_retained_earnings, 0) as period_beginning_balance,
        coalesce(br.cumulative_retained_earnings, 0) as period_ending_balance,
        coalesce(br.cumulative_retained_earnings_converted, 0) as period_beginning_converted_balance,
        coalesce(br.cumulative_retained_earnings_converted, 0) as period_ending_converted_balance,
        2 as sort_order
    from base_records br
    inner join retained_earnings_accounts rea 
        on br.source_relation = rea.source_relation
)

select *
from final
order by source_relation, period_first_day, sort_order