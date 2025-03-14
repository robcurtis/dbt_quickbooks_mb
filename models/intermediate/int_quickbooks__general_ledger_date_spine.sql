-- depends_on: {{ ref('quickbooks__general_ledger') }}

with spine as (

    {% if execute %}
    {% set first_date_query %}
        select  coalesce(min(cast(transaction_date as date)),cast({{ dbt.dateadd("month", -1, "current_date") }} as date)) as min_date from {{ ref('quickbooks__general_ledger') }}
    {% endset %}
    {% set first_date = run_query(first_date_query).columns[0][0]|string %}
    
        {% if target.type == 'postgres' %}
            {% set first_date_adjust = "cast('" ~ first_date[0:10] ~ "' as date)" %}

        {% else %}
            {% set first_date_adjust = "'" ~ first_date[0:10] ~ "'" %}

        {% endif %}

    {% else %} {% set first_date_adjust = "'2000-01-01'" %}
    {% endif %}

    {% if execute %}
    {% set last_date_query %}
        select  coalesce(max(cast(transaction_date as date)), cast(current_date as date)) as max_date from {{ ref('quickbooks__general_ledger') }}
    {% endset %}

    {% set current_date_query %}
        select current_date
    {% endset %}

    {% if run_query(current_date_query).columns[0][0]|string < run_query(last_date_query).columns[0][0]|string %}

    {% set last_date = run_query(last_date_query).columns[0][0]|string %}

    {% else %} {% set last_date = run_query(current_date_query).columns[0][0]|string %}
    {% endif %}
        
    {% if target.type == 'postgres' %}
        {% set last_date_adjust = "cast('" ~ last_date[0:10] ~ "' as date)" %}

    {% else %}
        {% set last_date_adjust = "'" ~ last_date[0:10] ~ "'" %}

    {% endif %}
    {% endif %}

    {{ dbt_utils.date_spine(
        datepart="month",
        start_date=first_date_adjust,
        end_date=dbt.dateadd("month", 1, last_date_adjust)
        )
    }}
),

general_ledger as (
    select *
    from {{ ref('quickbooks__general_ledger') }}
),

date_spine as (
    select
        cast({{ dbt.date_trunc("year", "date_month") }} as date) as date_year,
        cast({{ dbt.date_trunc("month", "date_month") }} as date) as period_first_day,
        {{ dbt.last_day("date_month", "month") }} as period_last_day,
        row_number() over (order by cast({{ dbt.date_trunc("month", "date_month") }} as date)) as period_index
    from spine
),

accounts as (
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

final as (
    select distinct
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_id
            ELSE general_ledger.account_id
        END as account_id,
        general_ledger.source_relation,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_number
            ELSE general_ledger.account_number
        END as account_number,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_name
            ELSE general_ledger.account_name
        END as account_name,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.is_sub_account
            ELSE general_ledger.is_sub_account
        END as is_sub_account,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.parent_account_number
            ELSE general_ledger.parent_account_number
        END as parent_account_number,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_id
            ELSE general_ledger.parent_account_name
        END as parent_account_name,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_id
            ELSE general_ledger.account_type
        END as account_type,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_id
            ELSE general_ledger.account_sub_type
        END as account_sub_type,
        CASE
            WHEN arc.cutover_date IS NOT NULL
            AND general_ledger.transaction_date IS NOT NULL
            AND general_ledger.transaction_date <= arc.cutover_date
            THEN dar.account_id
            ELSE general_ledger.account_class
        END as account_class,
        general_ledger.financial_statement_helper,
        general_ledger.class_id,
        date_spine.date_year,
        date_spine.period_first_day,
        date_spine.period_last_day,
        date_spine.period_index
    from general_ledger
    left join ar_cutover_date_matrix arc on general_ledger.source_relation = arc.source_relation and general_ledger.account_id = arc.account_id
    left join default_ar_account dar on general_ledger.source_relation = dar.source_relation

    cross join date_spine
)

select *
from final
