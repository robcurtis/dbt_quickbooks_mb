{{ config(
    materialized='incremental',
    unique_key=dbt_utils.generate_surrogate_key(['account_id', 'source_relation', 'calendar_date', 'class_id']),
    incremental_strategy='delete+insert',
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT IF NOT EXISTS pk_{{ this.identifier }} PRIMARY KEY (account_id, source_relation, calendar_date, class_id)"
    ]
) }}

with general_ledger_by_period as (

    select *
    from {{ ref('quickbooks__general_ledger_by_period') }}
    where financial_statement_helper = 'balance_sheet'
),  

final as (
    select
        period_first_day as calendar_date, --  Slated to be deprecated; we recommend using `period_first_day` or `period_last_day`
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
        account_number,
        account_id,
        account_name,
        period_ending_balance as amount,
        period_ending_converted_balance as converted_amount,
        account_ordinal
    from general_ledger_by_period
),

source_data as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['account_id', 'source_relation', 'calendar_date', 'class_id']) }} as dbt_row_id,
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