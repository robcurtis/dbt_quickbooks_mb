{{ config(
    materialized='incremental',
    unique_key='dbt_row_id',
    incremental_strategy='delete+insert',
    post_hook=after_commit("
      ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_{{ this.identifier }};
      ALTER TABLE {{ this }} ADD CONSTRAINT pk_{{ this.identifier }} PRIMARY KEY (dbt_row_id)
    "),
    on_schema_change='sync_all_columns'
) }}

with general_ledger_balances as (

    select *
    from {{ ref('int_quickbooks__general_ledger_balances') }}
    where NOT (account_number = '3900' and financial_statement_helper = 'balance_sheet') -- Replace with the calculated retained earnings
),

calculated_retained_earnings as (

    select *
    from {{ ref('int_quickbooks__retained_earnings') }}
),

{% if var('financial_statement_ordinal') %}
ordinals as ( 

    select 
        cast(account_class as {{ dbt.type_string() }}) as account_class,
        cast(account_type as {{ dbt.type_string() }}) as account_type,
        cast(account_sub_type as {{ dbt.type_string() }}) as account_sub_type,
        cast(account_number as {{ dbt.type_string() }}) as account_number,
        ordinal
    from {{ var('financial_statement_ordinal') }}
),
{% endif %}

balances_earnings_unioned as (

    select *
    from general_ledger_balances

    union all 

    select *
    from calculated_retained_earnings
), 

final as (

    select 
        balances_earnings_unioned.*,
    {% if var('financial_statement_ordinal') %}
        coalesce(account_number_ordinal.ordinal, account_sub_type_ordinal.ordinal, account_type_ordinal.ordinal, account_class_ordinal.ordinal) as account_ordinal
    {% else %}
        case 
            when account_class = 'Asset' then 1
            when account_class = 'Liability' then 2
            when account_class = 'Equity' then 3
            when account_class = 'Revenue' then 1
            when account_class = 'Expense' then 2
        end as account_ordinal 
    {% endif %}
    from balances_earnings_unioned
    {% if var('financial_statement_ordinal') %}
        left join ordinals as account_number_ordinal
            on balances_earnings_unioned.account_number = account_number_ordinal.account_number
            and balances_earnings_unioned.source_relation = account_number_ordinal.source_relation
        left join ordinals as account_sub_type_ordinal
            on balances_earnings_unioned.account_sub_type = account_sub_type_ordinal.account_sub_type
            and balances_earnings_unioned.source_relation = account_sub_type_ordinal.source_relation
        left join ordinals as account_type_ordinal
            on balances_earnings_unioned.account_type = account_type_ordinal.account_type
            and balances_earnings_unioned.source_relation = account_type_ordinal.source_relation
        left join ordinals as account_class_ordinal
            on balances_earnings_unioned.account_class = account_class_ordinal.account_class
            and balances_earnings_unioned.source_relation = account_class_ordinal.source_relation
    {% endif %}
),

source_data as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['account_id', 'account_class', 'source_relation', 'period_first_day', 'financial_statement_helper', 'parent_account_number']) }} as dbt_row_id,
        {{ dbt.current_timestamp() }} as dbt_updated_at
    from final
)

{% if is_incremental() and this.is_table_type %}
, last_update as (
    select max(dbt_updated_at) as max_dbt_updated_at 
    from {{ this }}
)

select source_data.*
from source_data, last_update
where source_data.dbt_updated_at >= last_update.max_dbt_updated_at

{% else %}

select *
from source_data

{% endif %}