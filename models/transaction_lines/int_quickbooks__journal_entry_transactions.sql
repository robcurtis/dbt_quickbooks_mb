--To disable this model, set the using_journal_entry variable within your dbt_project.yml file to False.
{{ config(enabled=var('using_journal_entry', True)) }}

with journal_entries as (

    select *
    from {{ ref('stg_quickbooks__journal_entry') }}
),

journal_entry_lines as (

    select *
    from {{ ref('stg_quickbooks__journal_entry_line') }}
),

accounts as (
    select *
    from {{ ref('stg_quickbooks__account') }}
),

default_ar_account as (
    select distinct
        first_value(account_id) over (
            partition by source_relation, currency_id
            order by account_number
        ) as default_account_id,
        currency_id,
        source_relation
    from accounts
    where account_sub_type = 'AccountsReceivable'
        and is_active
        and not is_sub_account
),

final as (

    select
        journal_entries.journal_entry_id as transaction_id,
        journal_entries.source_relation,
        journal_entry_lines.index as transaction_line_id,
        journal_entries.doc_number,
        'journal_entry' as transaction_type,
        journal_entries.transaction_date,
        coalesce(
            case 
                when acct.account_sub_type = 'AccountsReceivable' 
                and (acct.account_id is null or not acct.is_active)
                then default_ar.default_account_id
                else journal_entry_lines.account_id
            end,
            journal_entry_lines.account_id
        ) as account_id,
        journal_entry_lines.class_id,
        journal_entry_lines.department_id,
        journal_entry_lines.customer_id,
        journal_entry_lines.vendor_id,
        cast(billable_status as {{ dbt.type_string() }}) as billable_status,
        journal_entry_lines.description,
        case when lower(journal_entry_lines.posting_type) = 'credit'
            then journal_entry_lines.amount * -1 
            else journal_entry_lines.amount 
        end as amount,
        case when lower(journal_entry_lines.posting_type) = 'credit'
            then journal_entry_lines.amount * coalesce(-journal_entries.exchange_rate, -1)
            else journal_entry_lines.amount * coalesce(journal_entries.exchange_rate, 1)
        end as converted_amount,
        journal_entries.total_amount,
        journal_entries.total_amount * coalesce(journal_entries.exchange_rate, 1) as total_converted_amount
    from journal_entries

    left join journal_entry_lines
        on journal_entries.journal_entry_id = journal_entry_lines.journal_entry_id
        and journal_entries.source_relation = journal_entry_lines.source_relation

    left join accounts as acct
        on journal_entry_lines.account_id = acct.account_id
        and journal_entry_lines.source_relation = acct.source_relation

    left join default_ar_account as default_ar
        on journal_entry_lines.source_relation = default_ar.source_relation
        and coalesce(acct.currency_id, default_ar.currency_id) = default_ar.currency_id
)

select *
from final