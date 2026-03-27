{{
    config(
        materialized='view'
    )
}}

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

customer_agg AS (
    SELECT
        customer_id,
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_spend,
        AVG(amount) AS avg_order_value,
        MIN(transaction_date) AS first_purchase_date,
        MAX(transaction_date) AS last_purchase_date,
        COUNT(DISTINCT product_category) AS categories_purchased
    FROM transactions
    GROUP BY customer_id
)

SELECT * FROM customer_agg