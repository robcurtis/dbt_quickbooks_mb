{{ config(
    materialized='incremental',
    unique_key=['account_unique_id'],
    incremental_strategy='delete+insert',
    post_hook=[
      "ALTER TABLE {{ this }} ADD CONSTRAINT pk_{{ this.identifier }} PRIMARY KEY (account_unique_id)"
    ]
) }}

with cash_flow_classifications as (

    select *
    from {{ ref('int_quickbooks__cash_flow_classifications') }}
), 

final as (
    
    select cash_flow_classifications.*,
        coalesce(lag(cash_ending_period) over (partition by account_id, class_id, source_relation 
            order by source_relation, cash_flow_period), 0) as cash_beginning_period,
        cash_ending_period - coalesce(lag(cash_ending_period) over (partition by account_id, class_id, source_relation 
            order by source_relation, cash_flow_period), 0) as cash_net_period,
        coalesce(lag(cash_converted_ending_period) over (partition by account_id, class_id, source_relation 
            order by source_relation, cash_flow_period), 0) as cash_converted_beginning_period, 
        cash_converted_ending_period - coalesce(lag(cash_converted_ending_period) over (partition by account_id, class_id, source_relation 
            order by source_relation, cash_flow_period), 0) as cash_converted_net_period
    from cash_flow_classifications
),

source_data as (
    select 
        *,
        {{ dbt.current_timestamp() }} as dbt_updated_at
    from final
)

select *
from source_data
{% if is_incremental() %}
where dbt_updated_at >= (select max(dbt_updated_at) from {{ this }})
{% endif %}

