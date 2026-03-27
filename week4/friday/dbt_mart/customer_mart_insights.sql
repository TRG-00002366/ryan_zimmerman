-- Top 5 customers by lifetime value
SELECT
    customer_id,
    lifetime_value,
    avg_order_value,
    transaction_count,
    customer_segment,
    favorite_category
FROM {{ ref('fct_customer_metrics') }}
ORDER BY lifetime_value DESC, customer_id
LIMIT 5;

-- Segment distribution and total value per segment
SELECT
    customer_segment,
    COUNT(*) AS customer_count,
    ROUND(SUM(lifetime_value), 2) AS total_segment_value,
    ROUND(AVG(lifetime_value), 2) AS avg_customer_value
FROM {{ ref('fct_customer_metrics') }}
GROUP BY customer_segment
ORDER BY total_segment_value DESC, customer_segment;

-- Most popular favorite category
SELECT
    favorite_category,
    COUNT(*) AS customer_count
FROM {{ ref('fct_customer_metrics') }}
WHERE favorite_category IS NOT NULL
GROUP BY favorite_category
ORDER BY customer_count DESC, favorite_category
LIMIT 1;