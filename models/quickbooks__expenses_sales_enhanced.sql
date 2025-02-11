{{ config(
    materialized='incremental',
    unique_key=['dbt_row_id'],
    incremental_strategy='delete+insert',
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_{{ this.identifier }} PRIMARY KEY (dbt_row_id)"
    ]
) }}

with expenses as (

    select *
    from {{ ref('int_quickbooks__expenses_union') }}
),

{% if fivetran_utils.enabled_vars_one_true(['using_sales_receipt','using_invoice']) %}
sales as (

    select *
    from {{ ref('int_quickbooks__sales_union') }}
),
{% endif %}

final as (
    
    select *
    from expenses

    {% if fivetran_utils.enabled_vars_one_true(['using_sales_receipt','using_invoice']) %}
    union all

    select *
    from sales
    {% endif %}
),

source_data as (
    select 
        *,
        {{ dbt_utils.generate_surrogate_key(['transaction_id', 'source_relation', 'transaction_line_id', 'item_id']) }} as dbt_row_id,
        {{ dbt.current_timestamp() }} as dbt_updated_at
    from final
)

select *
from source_data
{% if is_incremental() %}
where dbt_updated_at >= (select max(dbt_updated_at) from {{ this }})
{% endif %}