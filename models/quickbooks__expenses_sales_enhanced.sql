{{ config(
    materialized='incremental',
    unique_key=['transaction_id', 'source_relation', 'transaction_line_id'],
    incremental_strategy='delete+insert',
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_{{ this.identifier }} PRIMARY KEY (transaction_id, source_relation, transaction_line_id)"
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
)

select *
from final