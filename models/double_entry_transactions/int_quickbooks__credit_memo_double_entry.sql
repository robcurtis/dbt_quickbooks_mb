/*
Table that creates a debit record to Discounts Refunds Given and a credit record to the specified income account.
*/

--To disable this model, set the using_credit_memo variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_credit_memo', True)) }}

with credit_memos as (

    select *
    from {{ ref('stg_quickbooks__credit_memo') }}
),

credit_memo_lines as (

    select *
    from {{ ref('stg_quickbooks__credit_memo_line') }}
),

items as (
    select
        item.*,
        parent.income_account_id as parent_income_account_id
    from {{ ref('stg_quickbooks__item') }} item

    left join {{ ref('stg_quickbooks__item') }} parent
        on item.parent_item_id = parent.item_id
        and item.source_relation = parent.source_relation
),

accounts as (

    select *
    from {{ ref('stg_quickbooks__account') }}
),

credit_memo_bundles as (

    select *
    from {{ ref('stg_quickbooks__credit_memo_line_bundle') }}
),

bundles as (

    select *
    from {{ ref('stg_quickbooks__bundle') }}
),

bundle_items as (

    select *
    from {{ ref('stg_quickbooks__bundle_item') }}
),

income_accounts as (

    select *
    from accounts

    where account_sub_type = '{{ var('quickbooks__sales_of_product_income_reference', 'SalesOfProductIncome') }}'
),

bundle_income_accounts as (

    select distinct
        coalesce(parent.income_account_id, income_accounts.account_id) as account_id,
        coalesce(parent.source_relation, income_accounts.source_relation) as source_relation,
        bundle_items.bundle_id

    from items

    left join items as parent
        on items.parent_item_id = parent.item_id
        and items.source_relation = parent.source_relation

    inner join income_accounts
        on income_accounts.account_id = items.income_account_id
        and income_accounts.source_relation = items.source_relation

    inner join bundle_items
        on bundle_items.item_id = items.item_id
        and bundle_items.source_relation = items.source_relation
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

credit_memo_join as (

    select
        credit_memos.credit_memo_id as transaction_id,
        credit_memos.source_relation,
        credit_memo_lines.index,
        credit_memos.transaction_date,
        credit_memo_lines.amount,
        (credit_memo_lines.amount * coalesce(credit_memos.exchange_rate, 1)) as converted_amount,
        coalesce(
            credit_memo_bundles.account_id,
            credit_memo_lines.sales_item_account_id, 
            items.income_account_id, 
            items.expense_account_id
        ) as account_id,
        credit_memos.customer_id,
        coalesce(
            credit_memo_bundles.class_id,
            credit_memo_lines.sales_item_class_id, 
            credit_memo_lines.discount_class_id, 
            credit_memos.class_id
        ) as class_id,
        credit_memos.department_id,
        credit_memos.currency_id,
        credit_memos.created_at,
        credit_memos.updated_at

    from credit_memos

    inner join credit_memo_lines
        on credit_memos.credit_memo_id = credit_memo_lines.credit_memo_id
        and credit_memos.source_relation = credit_memo_lines.source_relation

    left join items
        on credit_memo_lines.sales_item_item_id = items.item_id
        and credit_memo_lines.source_relation = items.source_relation
    
    left join credit_memo_bundles
        on credit_memo_lines.credit_memo_id = credit_memo_bundles.credit_memo_id
        and credit_memo_lines.index = credit_memo_bundles.credit_memo_line_index
        and credit_memo_lines.source_relation = credit_memo_bundles.source_relation

    where coalesce(
        credit_memo_bundles.account_id,
        credit_memo_lines.discount_account_id, 
        credit_memo_lines.sales_item_account_id, 
        credit_memo_lines.sales_item_item_id) is not null
),

final as (

    select
        transaction_id,
        credit_memo_join.source_relation,
        index,
        transaction_date,
        customer_id,
        cast(null as {{ dbt.type_string() }}) as vendor_id,
        amount * -1 as amount,
        converted_amount * -1 as converted_amount,
        account_id,
        class_id,
        department_id,
        created_at,
        updated_at,
        'credit' as transaction_type,
        'credit_memo' as transaction_source
    from credit_memo_join

    union all

    select
        transaction_id,
        credit_memo_join.source_relation,
        index,
        transaction_date,
        customer_id,
        cast(null as {{ dbt.type_string() }}) as vendor_id,
        amount * -1 as amount,
        converted_amount * -1 as converted_amount,
        ar_accounts.account_id,
        class_id,
        department_id,
        created_at,
        updated_at,
        'debit' as transaction_type,
        'credit_memo' as transaction_source
    from credit_memo_join

    left join ar_accounts
        on ar_accounts.source_relation = credit_memo_join.source_relation
)

select *
from final