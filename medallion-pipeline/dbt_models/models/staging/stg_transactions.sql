-- Staging model: light renaming and casting from raw source
-- Materialized as view — no storage cost, always fresh

{{ config(materialized='view') }}

with source as (
    select * from {{ source('finance_raw', 'transactions') }}
),

staged as (
    select
        transaction_id                              as transaction_id,
        trim(upper(customer_id))                    as customer_id,
        cast(amount as numeric(18, 2))              as amount,
        trim(upper(currency))                       as currency,
        cast(transaction_date as timestamp)         as transaction_ts,
        trim(lower(transaction_type))               as transaction_type,
        status,
        _ingestion_timestamp,
        _source_file

    from source
    where transaction_id is not null
      and amount is not null
)

select * from staged
