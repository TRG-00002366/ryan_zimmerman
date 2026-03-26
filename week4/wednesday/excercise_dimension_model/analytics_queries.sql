USE DATABASE RYAN_DEV_DB;
USE SCHEMA GOLD;

--1: Revenue by Month
SELECT 
    d.year,
    d.month_name,
    SUM(f.revenue) AS total_revenue,
    COUNT(DISTINCT f.transaction_id) AS transaction_count
FROM FCT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.year, d.month_name, d.month_num
ORDER BY d.year, d.month_num;

--2: Revenue by Customer Tier: Show total revenue and average order value by loyalty tier.
SELECT
    c.loyalty_tier AS tier,
    SUM(f.revenue) AS total_rev,
    AVG(f.revenue) AS average_order_value
FROM FCT_SALES f
INNER JOIN DIM_CUSTOMER c ON f.customer_key = c.customer_key
GROUP BY c.loyalty_tier;

--3: Top Products: List top 5 products by revenue.
SELECT 
    p.product_name AS product,
    SUM(f.revenue) AS total_revenue
FROM FCT_SALES f
INNER JOIN DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC
LIMIT 5;
    


--4: Store Performance: Compare online vs retail store revenue.
SELECT
    s.type AS type,
    SUM(f.revenue) AS total_revenue
FROM FCT_SALES f
INNER JOIN DIM_STORE s ON f.store_key = s.store_key
GROUP BY s.type;

--5: Weekend vs Weekday: Is there a difference in sales patterns?
SELECT 
    d.is_weekend AS weekend,
    CASE 
        WHEN d.is_weekend = TRUE THEN AVG(f.revenue) / 2
        ELSE AVG(f.revenue) / 5
    END AS daily_average_revenue
FROM FCT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.is_weekend

CREATE OR REPLACE VIEW V_DAILY_SALES_SUMMARY AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    SUM(f.revenue) AS daily_revenue,
    SUM(f.quantity) AS units_sold,
    COUNT(DISTINCT f.customer_key) AS unique_customers
FROM FCT_SALES f
JOIN DIM_DATE d ON f.date_key = d.date_key
GROUP BY d.full_date, d.day_name, d.is_weekend;

CREATE OR REPLACE VIEW V_TOP_PRODUCTS AS
SELECT 
    p.product_name AS product,
    SUM(f.revenue) AS total_revenue
FROM FCT_SALES f
INNER JOIN DIM_PRODUCT p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC