{{ config(
    materialized='incremental',
    unique_key='unique_id',
    incremental_strategy='delete+insert',
    post_hook=after_commit("
      ALTER TABLE {{ this }} DROP CONSTRAINT IF EXISTS pk_{{ this.identifier }};
      ALTER TABLE {{ this }} ADD CONSTRAINT pk_{{ this.identifier }} PRIMARY KEY (unique_id)
    ")
) }}

with accounts as (
    select * from {{ ref('stg_quickbooks__account') }}
),

accounts_classification as (
    select * from {{ ref('int_quickbooks__account_classifications') }}
),

default_ar_account as (
    select a.*
    from (
        select distinct
        first_value(a.account_id) over (
            partition by a.source_relation, a.currency_id
            order by a.updated_at desc
        ) as default_account_id,
        currency_id,
        source_relation
    from {{ ref('stg_quickbooks__account') }} a
        where account_sub_type = 'AccountsReceivable'
        and is_active
        and not is_sub_account) t1
    left join accounts_classification a on t1.source_relation = a.source_relation and t1.default_account_id = a.account_id
    where account_sub_type = 'AccountsReceivable'
        and is_active
        and not is_sub_account
),

inactive_dates as (
    select * from {{ ref('int_quickbooks__ar_inactive_dates') }}
),

ar_cutover_date_pre_matrix as (
select
    case
        when i.first_inactive_date is not null
        and i.last_active_date >= i.first_inactive_date
        then i.first_inactive_date
    end as cutover_date,
    i.first_inactive_date,
    i.last_active_date,
    a.*
from {{ ref('stg_quickbooks__account') }} a
left join inactive_dates i
    on a.account_id = i.account_id
    and a.source_relation = i.source_relation
where a.account_sub_type = 'AccountsReceivable'
),

ar_cutover_date_matrix as (
select * from ar_cutover_date_pre_matrix a
where a.is_active and a.is_sub_account and a.cutover_date IS NOT NULL
order by a.source_relation, a.account_number
),

-- select * from ar_cutover_date_matrix

stgd_general_ledger as (
select
    gl.unique_id,
    gl.transaction_id,
    gl.source_relation,
    gl.transaction_index,
    gl.transaction_date,
    gl.customer_id,
    gl.vendor_id,
    gl.amount,
    CASE
        WHEN gl.transaction_date <= arc.cutover_date
        THEN dar.account_id
        ELSE gl.account_id
    END as account_id,
    gl.class_id,
    gl.department_id,
    CASE
        WHEN gl.transaction_date <= arc.cutover_date
        THEN dar.account_number
        ELSE gl.account_number
    END as account_number,
    CASE
        WHEN gl.transaction_date <= arc.cutover_date
        THEN dar.name
        ELSE gl.account_name
    END as account_name,
    CASE WHEN gl.transaction_date <= arc.cutover_date
        THEN dar.is_sub_account
        ELSE gl.is_sub_account
    END as is_sub_account,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.parent_account_number
        ELSE gl.parent_account_number
    END as parent_account_number,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.parent_account_name
        ELSE gl.parent_account_name
    END as parent_account_name,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.account_type
        ELSE gl.account_type
    END as account_type,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.account_sub_type
        ELSE gl.account_sub_type
    END as account_sub_type,
    gl.financial_statement_helper,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.balance
        ELSE gl.account_current_balance
    END as account_current_balance,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.classification
        ELSE gl.account_class
    END as account_class,
    gl.transaction_type,
    gl.transaction_source,
    CASE when gl.transaction_date <= arc.cutover_date
        THEN dar.transaction_type
        ELSE gl.account_transaction_type
    END as account_transaction_type,
    gl.created_at,
    gl.updated_at,
    gl.adjusted_amount,
    gl.adjusted_converted_amount
from {{ ref('int_quickbooks__general_ledger') }} gl
left join ar_cutover_date_matrix arc on gl.source_relation = arc.source_relation and gl.account_id = arc.account_id
left join default_ar_account dar on gl.source_relation = dar.source_relation
),

final as (select *,
                 sum(adjusted_amount) over (partition by account_id, class_id, source_relation
                     order by source_relation, transaction_date, account_id, class_id rows unbounded preceding) as running_balance,
                 sum(adjusted_converted_amount) over (partition by account_id, class_id, source_relation
                     order by source_relation, transaction_date, account_id, class_id rows unbounded preceding) as running_converted_balance
          from stgd_general_ledger
),

source_data as (
    select 
        *,
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
