## Table: web_events

### Source Information
| Attribute              | Value                                              |
| ---------------------- | -------------------------------------------------- |
| Source Bronze Table    | BRONZE.raw_web_events                              |
| Primary Key            | event_id                                           |
| Deduplication Strategy | Keep latest record per event_id using ingestion_ts |


### Column Definitions

| Column Name     | Data Type     | Source Expression                      | Transformation          |
| --------------- | ------------- | -------------------------------------- | ----------------------- |
| event_id        | STRING        | raw_data:event_id::STRING              | Extract from JSON       |
| event_type      | STRING        | raw_data:event_type::STRING            | Lowercase               |
| event_timestamp | TIMESTAMP_NTZ | raw_data:timestamp::TIMESTAMP_NTZ      | Convert to timestamp    |
| user_id         | STRING        | raw_data:user_id::STRING               | Extract from JSON       |
| session_id      | STRING        | raw_data:session_id::STRING            | Extract from JSON       |
| page_url        | STRING        | raw_data:page_url::STRING              | Trim whitespace         |
| device          | STRING        | raw_data:device::STRING                | Lowercase               |
| product_id      | STRING        | raw_data:properties.product_id::STRING | Nested JSON extraction  |



### Transformations Applied

1. **Data Typing:**
Convert timestamp string → TIMESTAMP_NTZ
Extract all JSON fields to STRING

2. **Standardization:**
event_type, device → lowercase
Trim page_url

3. **Null Handling:**
Missing product_id allowed (NULL)
Replace missing device with 'unknown'

4. **Deduplication:**
Only keep the most recent of duplicated rows by assigning row numbers based on ingestion_ts.

### DDL Statement

```sql
CREATE OR REPLACE TABLE SILVER.web_events (
    event_id STRING,
    
    event_type STRING,
    event_timestamp TIMESTAMP_NTZ,
    user_id STRING,
    session_id STRING,
    page_url STRING,
    device STRING,
    product_id STRING,
    
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (event_id)
)
COMMENT = 'Cleaned and structured web events data';
```

### Sample Transformation Query

```sql
SELECT *
FROM (
    SELECT
        raw_data:event_id::STRING AS event_id,
        LOWER(raw_data:event_type::STRING) AS event_type,
        raw_data:timestamp::TIMESTAMP_NTZ AS event_timestamp,
        raw_data:user_id::STRING AS user_id,
        raw_data:session_id::STRING AS session_id,
        TRIM(raw_data:page_url::STRING) AS page_url,
        COALESCE(LOWER(raw_data:device::STRING), 'unknown') AS device,
        raw_data:properties.product_id::STRING AS product_id,
        ingestion_ts,
        ROW_NUMBER() OVER (PARTITION BY raw_data:event_id::STRING ORDER BY ingestion_ts DESC) AS rn
    FROM BRONZE.raw_web_events
)
WHERE rn = 1;
```




## Table: orders

### Source Information
| Attribute              | Value                           |
| ---------------------- | ------------------------------- |
| Source Bronze Table    | BRONZE.raw_orders               |
| Primary Key            | order_id                        |
| Deduplication Strategy | Keep latest record per order_id |


### Column Definitions

| Column Name    | Data Type    | Source Expression               | Transformation  |
| -------------- | ------------ | ------------------------------- | --------------- |
| order_id       | STRING       | order_id                        | Direct          |
| customer_id    | STRING       | customer_id                     | Direct          |
| product_id     | STRING       | product_id                      | Direct          |
| order_date     | DATE         | raw_data:order_date::DATE       | Convert to DATE |
| status         | STRING       | raw_data:status::STRING         | Uppercase       |
| total_amount   | NUMBER(10,2) | raw_data:total_amount::NUMBER   | Cast numeric    |
| payment_method | STRING       | raw_data:payment_method::STRING | Uppercase       |


### Transformations Applied

1. **Data Typing:**
order_date → DATE
total_amount → NUMBER(10,2)

2. **Standardization:**
status, payment_method → UPPERCASE

3. **Null Handling:**
total_amount default to 0 if NULL
status default to 'UNKNOWN'

4. **Deduplication:**
Only keep the most recent of duplicated rows by assigning row numbers based on ingestion_ts.

### DDL Statement

```sql
CREATE OR REPLACE TABLE SILVER.orders (
    order_id STRING,
    
    customer_id STRING,
    product_id STRING,
    order_date DATE,
    status STRING,
    total_amount NUMBER(10,2),
    payment_method STRING,
    
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (order_id)
)
COMMENT = 'Cleaned and structured order transactions';
```

### Sample Transformation Query

```sql
SELECT *
FROM (
    SELECT
        order_id,
        customer_id,
        product_id,
        raw_data:order_date::DATE AS order_date,
        COALESCE(UPPER(raw_data:status::STRING), 'UNKNOWN') AS status,
        COALESCE(raw_data:total_amount::NUMBER(10,2), 0) AS total_amount,
        UPPER(raw_data:payment_method::STRING) AS payment_method,
        ingestion_ts,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingestion_ts DESC) AS rn
    FROM BRONZE.raw_orders
)
WHERE rn = 1;
```




## Table: products

### Source Information
| Attribute              | Value                        |
| ---------------------- | ---------------------------- |
| Source Bronze Table    | BRONZE.raw_products          |
| Primary Key            | product_id                   |
| Deduplication Strategy | Latest record per product_id |


### Column Definitions

| Column Name     | Data Type    | Source Expression                | Transformation |
| --------------- | ------------ | -------------------------------- | -------------- |
| product_id      | STRING       | product_id                       | Direct         |
| name            | STRING       | name                             | Trim           |
| category        | STRING       | raw_data:category::STRING        | Uppercase      |
| price           | NUMBER(10,2) | raw_data:price::NUMBER           | Cast numeric   |
| inventory_count | NUMBER       | raw_data:inventory_count::NUMBER | Cast           |


### Transformations Applied

1. **Data Typing:**
price → NUMBER(10,2)
inventory_count → NUMBER

2. **Standardization:**
category → UPPERCASE
name → trimmed

3. **Null Handling:**
inventory_count → 0 if NULL

4. **Deduplication:**
Only keep the most recent of duplicated rows by assigning row numbers based on ingestion_ts.

### DDL Statement

```sql
CREATE OR REPLACE TABLE SILVER.products (
    product_id STRING,
    
    name STRING,
    category STRING,
    price NUMBER(10,2),
    inventory_count NUMBER,
    
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (product_id)
)
COMMENT = 'Cleaned and structured product catalog';
```

### Sample Transformation Query

```sql
SELECT *
FROM (
    SELECT
        product_id,
        TRIM(name) AS name,
        UPPER(raw_data:category::STRING) AS category,
        raw_data:price::NUMBER(10,2) AS price,
        COALESCE(raw_data:inventory_count::NUMBER, 0) AS inventory_count,
        ingestion_ts,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY ingestion_ts DESC) AS rn
    FROM BRONZE.raw_products
)
WHERE rn = 1;
```




## Table: customers

### Source Information
| Attribute              | Value                         |
| ---------------------- | ----------------------------- |
| Source Bronze Table    | BRONZE.raw_customers          |
| Primary Key            | customer_id                   |
| Deduplication Strategy | Latest record per customer_id |


### Column Definitions

| Column Name  | Data Type | Source Expression           | Transformation   |
| ------------ | --------- | --------------------------- | ---------------- |
| customer_id  | STRING    | customer_id                 | Direct           |
| email        | STRING    | email                       | Lowercase + trim |
| created_date | DATE      | raw_data:created_date::DATE | Convert to DATE  |


### Transformations Applied

1. **Data Typing:**
created_date → DATE

2. **Standardization:**
email → lowercase + trimmed

3. **Null Handling:**
email → NULL if invalid format

4. **Deduplication:**
Only keep the most recent of duplicated rows by assigning row numbers based on ingestion_ts.


### DDL Statement

```sql
CREATE OR REPLACE TABLE SILVER.customers (
    customer_id STRING,
    
    email STRING,
    created_date DATE,
    
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    
    PRIMARY KEY (customer_id)
)
COMMENT = 'Cleaned and standardized customer profiles';
```

### Sample Transformation Query

```sql
SELECT *
FROM (
    SELECT
        customer_id,
        LOWER(TRIM(email)) AS email,
        raw_data:created_date::DATE AS created_date,
        ingestion_ts,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY ingestion_ts DESC) AS rn
    FROM BRONZE.raw_customers
)
WHERE rn = 1;
```