-- Fact table: daily transaction aggregations per currency
-- Used by Power BI dashboards and regulatory reporting
-- Materialized as incremental table on transaction_date

{{
    config(
        materialized='incremental',
        unique_key='surrogate_key',
        on_schema_change='sync_all_columns'
    )
}}

with transactions as (
    select * from {{ ref('stg_transactions') }}

    {% if is_incremental() %}
    where transaction_ts >= (select max(transaction_date) from {{ this }})
    {% endif %}
),

daily_agg as (
    select
        date_trunc('day', transaction_ts)           as transaction_date,
        currency,
        count(transaction_id)                       as transaction_count,
        sum(amount)                                 as total_amount,
        avg(amount)                                 as avg_amount,
        max(amount)                                 as max_amount,
        min(amount)                                 as min_amount,
        count(distinct customer_id)                 as unique_customers,
        count(case when amount >= 10000 then 1 end) as high_value_count,
        current_timestamp()                         as _dbt_updated_at

    from transactions
    group by 1, 2
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['transaction_date', 'currency']) }} as surrogate_key,
        *
    from daily_agg
)

select * from final
