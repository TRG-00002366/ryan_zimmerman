
{{
	config(
		materialized='table'
	)
}}

WITH customer_txn AS (
	SELECT * FROM {{ ref('int_customer_transactions') }}
),

category_rank AS (
	SELECT 
		customer_id,
		product_category,
		SUM(amount) AS category_spend,
		ROW_NUMBER() OVER (
			PARTITION BY customer_id 
			ORDER BY SUM(amount) DESC
		) AS rank
	FROM {{ ref('stg_transactions') }}
	GROUP BY customer_id, product_category
),

favorite_category AS (
	SELECT customer_id, product_category AS favorite_category
	FROM category_rank
	WHERE rank = 1
),

final AS (
	SELECT
		c.customer_id,
		c.transaction_count,
		c.total_spend,
		ROUND(c.avg_order_value, 2) AS avg_order_value,
		c.first_purchase_date,
		c.last_purchase_date,
		DATEDIFF('day', c.first_purchase_date, CURRENT_DATE()) AS customer_age_days,
		DATEDIFF('day', c.last_purchase_date, CURRENT_DATE()) AS days_since_last_purchase,
		c.categories_purchased,
		f.favorite_category,
		CASE
			WHEN c.total_spend >= 500 THEN 'High Value'
			WHEN c.total_spend >= 200 THEN 'Medium Value'
			ELSE 'Low Value'
		END AS customer_segment,
		CURRENT_TIMESTAMP() AS _refreshed_at
	FROM customer_txn c
	LEFT JOIN favorite_category f ON c.customer_id = f.customer_id
)

SELECT * FROM final
