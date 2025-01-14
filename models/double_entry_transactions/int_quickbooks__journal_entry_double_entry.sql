/*
Table that provides the debit and credit records of a journal entry transaction.
*/

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
            order by updated_at desc
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
        journal_entry_lines.index,
        journal_entries.transaction_date,
        journal_entry_lines.customer_id,
        journal_entry_lines.vendor_id,
        journal_entry_lines.amount,
        (journal_entry_lines.amount * coalesce(journal_entries.exchange_rate, 1)) as converted_amount,
        journal_entry_lines.account_id
        class_id,
        journal_entry_lines.department_id,
        journal_entries.created_at,
        journal_entries.updated_at,
        lower(journal_entry_lines.posting_type) as transaction_type,
        'journal_entry' as transaction_source
    from journal_entries

    inner join journal_entry_lines
        on journal_entries.journal_entry_id = journal_entry_lines.journal_entry_id
        and journal_entries.source_relation = journal_entry_lines.source_relation

    left join accounts as acct
        on journal_entry_lines.account_id = acct.account_id
        and journal_entry_lines.source_relation = acct.source_relation

    left join default_ar_account as default_ar
        on journal_entry_lines.source_relation = default_ar.source_relation
        and coalesce(acct.currency_id, default_ar.currency_id) = default_ar.currency_id

    where journal_entry_lines.amount is not null
)

select *
from final
