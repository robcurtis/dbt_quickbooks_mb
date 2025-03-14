/*
Table that creates a debit record to either undeposited funds or a specified cash account and a credit record to accounts receivable.
*/

--To disable this model, set the using_payment variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_payment', True)) }}

with payments as (

    select *
    from {{ ref('stg_quickbooks__payment') }}
),

payment_lines as (

    select *
    from {{ ref('stg_quickbooks__payment_line') }}
),

accounts as (

    select *
    from {{ ref('stg_quickbooks__account') }}
),

ar_accounts as (

    select distinct
        first_value(account_id) over (
            partition by source_relation, currency_id
            order by updated_at desc
        ) as account_id,
        currency_id,
        source_relation
    from accounts
    
    where account_type = '{{ var('quickbooks__accounts_receivable_reference', 'Accounts Receivable') }}'
        and is_active
        and not is_sub_account
),

payment_join as (

    select
        payments.payment_id as transaction_id,
        payments.source_relation,
        row_number() over(partition by payments.payment_id, payments.source_relation 
            order by payments.source_relation, payments.transaction_date) - 1 as index,
        payments.transaction_date,
        payments.total_amount as amount,
        (payments.total_amount * coalesce(payments.exchange_rate, 1)) as converted_amount,
        payments.deposit_to_account_id,
        payments.receivable_account_id,
        payments.customer_id,
        payments.currency_id,
        payments.created_at,
        payments.updated_at
    from payments
),

final as (
    select
        transaction_id,
        payment_join.source_relation,
        index,
        transaction_date,
        customer_id,
        cast(null as {{ dbt.type_string() }}) as vendor_id,
        amount,
        converted_amount,
        coalesce(deposit_to_account_id, 
            {{ var('quickbooks__undeposited_funds_account_id', "'UNDEPOSITED_FUNDS'") }}) as account_id,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast(null as {{ dbt.type_string() }}) as department_id,
        created_at,
        updated_at,
        'debit' as transaction_type,
        'payment' as transaction_source
    from payment_join

    union all

    select
        transaction_id,
        payment_join.source_relation,
        index,
        transaction_date,
        customer_id,
        cast(null as {{ dbt.type_string() }}) as vendor_id,
        amount,
        converted_amount,
        coalesce(payment_join.receivable_account_id, 
            case when payment_join.receivable_account_id is null then ar_accounts.account_id end) as account_id,
        cast(null as {{ dbt.type_string() }}) as class_id,
        cast(null as {{ dbt.type_string() }}) as department_id,
        created_at,
        updated_at,
        'credit' as transaction_type,
        'payment' as transaction_source
    from payment_join

    left join ar_accounts
        on ar_accounts.source_relation = payment_join.source_relation
)

select *
from final
