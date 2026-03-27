{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_transactions') }}
),

transformed AS (
    SELECT
        raw_data:transaction_id::STRING AS transaction_id,
        raw_data:customer_id::STRING AS customer_id,
        raw_data:transaction_date::DATE AS transaction_date,
        raw_data:amount::DECIMAL(10,2) AS amount,
        raw_data:product_category::STRING AS product_category,
        _loaded_at
    FROM source
)

SELECT * FROM transformed