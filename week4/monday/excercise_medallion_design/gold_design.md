## Table: daily_sales_summary

Purpose
Table that provides a daily overview of sales performance for business stakeholders.

Business Questions Answered
How much revenue are we generating each day?
How many orders are placed daily?
What is the average order value over time?

Aggregations / Joins Required
Group by order_date
SUM(total_amount) → total revenue
COUNT(order_id) → order count
AVG(total_amount) → average order value

### Source Information
| Attribute              | Value                         |
| ---------------------- | ----------------------------- |
| Source Silver Table    | SILVER.orders                 |
| Primary Key            | order_Date                    |




### Column Definitions

| Column Name     | Data Type    | Description           |
| --------------- | ------------ | --------------------- |
| order_date      | DATE         | Date of orders        |
| total_revenue   | NUMBER(12,2) | Total revenue per day |
| order_count     | NUMBER       | Number of orders      |
| avg_order_value | NUMBER(10,2) | Average order value   |




### DDL Statement

```sql
CREATE OR REPLACE TABLE GOLD.daily_sales_summary AS
SELECT
    order_date,
    SUM(total_amount) AS total_revenue,
    COUNT(order_id) AS order_count,
    AVG(total_amount) AS avg_order_value
FROM SILVER.orders
WHERE status = 'COMPLETED'
GROUP BY order_date;
```



## Table: customer_360_view

Purpose
Table that provides insight into customers for sales representatives.

Business Questions Answered
How much value does each customer provide?
What category of products do our customers purchase from?
How often do they order?
How long has it been since they ordered? (Track inactive/active customers)

Aggregations / Joins Required
JOIN  orders, products on product_id
GROUP BY customer_id
SUM(total_amount) → lifetime value
COUNT(order_id) → order count
MAX(order_date) → most recent order
Most frequent category (COUNT + MAX)

### Source Information
| Attribute              | Value                         |
| ---------------------- | ----------------------------- |
| Source Silver Table    | SILVER.orders SILVER.products |
| Primary Key            | customer_id                   |




### Column Definitions

| Column Name          | Data Type    | Description                    |
| -------------------- | ------------ | ------------------------------ |
| customer_id          | STRING       | Customer identifier            |
| total_lifetime_value | NUMBER(12,2) | Total spend                    |
| order_count          | NUMBER       | Number of orders               |
| last_order_date      | DATE         | Most recent purchase           |
| favorite_category    | STRING       | Most viewed/purchased category |





### DDL Statement

```sql
CREATE OR REPLACE TABLE GOLD.customer_360_view AS
WITH favorite_catagory AS (
    SELECT
        o.customer_id,
        p.category,
        MAX(COUNT(*)) AS cat_count
    FROM SILVER.orders o
    INNER JOIN SILVER.products p ON o.product_id = p.product_id
    WHERE o.status = 'COMPLETED'
    GROUP BY o.customer_id, p.category
)
SELECT
    customer_id,
    SUM(total_amount) AS total_lifetime_value,
    COUNT(order_id) AS order_count,
    MAX(order_date) AS last_order_date,
    fc.category
FROM SILVER.orders as o
INNER JOIN SILVER.products as p
ON o.product_id = p.product_id
INNER JOIN favorite_catagory as fc
ON fc.customer_id = o.customer_id
WHERE status = 'COMPLETED'
GROUP BY customer_id;
```