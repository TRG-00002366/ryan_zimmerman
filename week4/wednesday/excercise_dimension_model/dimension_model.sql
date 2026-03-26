USE DATABASE RYAN_DEV_DB;
USE SCHEMA GOLD;

CREATE OR REPLACE TABLE DIM_DATE AS
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD')) AS date_key,
    d AS full_date,
    DAY(d) AS day_of_month,
    DAYOFWEEK(d) AS day_of_week,
    DAYNAME(d) AS day_name,
    MONTH(d) AS month_num,
    MONTHNAME(d) AS month_name,
    QUARTER(d) AS quarter,
    YEAR(d) AS year,
    CASE WHEN DAYOFWEEK(d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT DATEADD('day', SEQ4(), '2020-01-01')::DATE AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 2191))
);

SELECT * FROM DIM_DATE WHERE year = 2024 LIMIT 10;

-- DIM_CUSTOMER
CREATE OR REPLACE TABLE DIM_CUSTOMER (
    customer_key INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_id STRING,  -- Natural key
    customer_name STRING,
    loyalty_tier STRING,
    city STRING,
    state STRING,
    country STRING,
    registration_date DATE,
    effective_date DATE DEFAULT CURRENT_DATE(),
    is_current BOOLEAN DEFAULT TRUE
);

-- Insert sample data
INSERT INTO DIM_CUSTOMER (customer_id, customer_name, loyalty_tier, city, state, country, registration_date)
VALUES 
    ('C001', 'John Smith', 'Gold', 'New York', 'NY', 'USA', '2023-01-15'),
    ('C002', 'Jane Doe', 'Silver', 'Los Angeles', 'CA', 'USA', '2023-03-20'),
    ('C003', 'Bob Wilson', 'Bronze', 'Chicago', 'IL', 'USA', '2023-06-01');


-- DIM_PRODUCT
CREATE OR REPLACE TABLE DIM_PRODUCT (
    product_key INTEGER AUTOINCREMENT PRIMARY KEY,
    product_id STRING,  -- Natural key
    product_name STRING,
    category STRING,
    subcategory STRING,
    publisher STRING,
    release_date DATE DEFAULT CURRENT_DATE(),
    price DECIMAL(10,2),
    is_available BOOLEAN DEFAULT TRUE
);


INSERT INTO DIM_PRODUCT (product_id, product_name, category, subcategory, publisher, release_date, price) VALUES
    ('P001', 'Widget Pro', 'Electronics', 'Gadgets', 'Acme Corp', '2023-01-15', 199.99),
    ('P002', 'BookMaster', 'Books', 'Fiction', 'BookHouse', '2024-01-15', 14.99),
    ('P003', 'GameBox', 'Games', 'Console', 'FunGames', '2025-01-15', 299.99),
    ('P004', 'SmartLamp', 'Home', 'Lighting', 'BrightHome', '2026-01-15', 49.99),
    ('P005', 'EcoBottle', 'Outdoors', 'Accessories', 'GreenLife', '2023-01-16', 24.99);


-- DIM_PRODUCT
CREATE OR REPLACE TABLE DIM_STORE (
    store_key INTEGER AUTOINCREMENT PRIMARY KEY,
    store_id STRING,  -- Natural key
    store_name STRING,
    region STRING,
    country STRING,
    type STRING,
    is_open BOOLEAN DEFAULT TRUE --Only False if store has permenantly closed
);


INSERT INTO DIM_STORE (store_id, store_name, region, country, type) VALUES
    ('S001', 'Central Market', 'East', 'USA', 'retail'),
    ('S002', 'Book Nook', 'West', 'USA', 'retail'),
    ('S003', 'Gadget World', 'Midwest', 'USA', 'retail'),
    ('S004', 'Game Center', 'South', 'USA', 'online'),
    ('S005', 'Outdoor Hub', 'West', 'USA', 'retail');


CREATE OR REPLACE TABLE FCT_SALES (
    -- Degenerate dimension (transaction ID)
    transaction_id STRING,
    
    -- Foreign keys to dimensions
    date_key INTEGER,
    customer_key INTEGER,
    product_key INTEGER,
    store_key INTEGER,
    
    -- Measures (additive facts)
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    discount_percent DECIMAL(5,2),
    revenue DECIMAL(12,2),
    discount_amount DECIMAL(10,2),
    
    -- Timestamps
    loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert sample transactions
INSERT INTO FCT_SALES (transaction_id, date_key, customer_key, product_key, store_key, 
                       quantity, unit_price, discount_percent, revenue, discount_amount)
VALUES
    ('T001', 20240115, 1, 1, 1, 2, 59.99, 10, 107.98, 12.00),
    ('T002', 20240115, 2, 2, 2, 1, 29.99, 0, 29.99, 0),
    ('T003', 20240116, 1, 3, 1, 3, 49.99, 15, 127.47, 22.50);

INSERT INTO FCT_SALES (transaction_id, date_key, customer_key, product_key, store_key, 
                       quantity, unit_price, discount_percent, revenue, discount_amount)
VALUES
    ('T004', 20240116, 3, 4, 3, 1, 199.99, 5, 189.99, 10.00),
    ('T005', 20240117, 3, 2, 2, 2, 29.99, 0, 59.98, 0),
    ('T006', 20240117, 2, 5, 4, 1, 24.99, 20, 19.99, 5.00),
    ('T007', 20240118, 2, 1, 1, 1, 59.99, 10, 53.99, 6.00),
    ('T008', 20240118, 1, 3, 5, 2, 49.99, 0, 99.98, 0),
    ('T009', 20240119, 3, 4, 3, 1, 199.99, 15, 169.99, 30.00),
    ('T010', 20240119, 3, 2, 2, 3, 29.99, 5, 28.49, 1.50),
    ('T011', 20240120, 1, 5, 4, 2, 24.99, 0, 24.99, 0),
    ('T012', 20240120, 2, 4, 3, 1, 199.99, 10, 179.99, 20.00),
    ('T013', 20240121, 3, 1, 1, 2, 59.99, 0, 119.98, 0);