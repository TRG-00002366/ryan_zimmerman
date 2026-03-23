## Table: raw_web_events

### Source Information
| Attribute         | Value                 |
|-------------------|-----------------------|
| Source System     | website               |
| Data Format       | JSON                  |
| Update Frequency  | Continuous            |
| Load Method       | Streaming ingestion   |

### Column Definitions

| Column Name     | Data Type     | Description                 | Sample Value                |
| --------------- | ------------- | --------------------------- | --------------------------- |
| ingestion_ts    | TIMESTAMP_NTZ | When data was loaded        | 2024-01-15 10:30:00         |
| source_file     | STRING        | Source file or stream batch | events_batch_001.json       |
| raw_data        | VARIANT       | Full raw event payload      | {"event_id":"e-abc123",...} |
| event_id        | STRING        | Extracted event identifier  | e-abc123                    |
| event_type      | STRING        | Type of event               | page_view                   |



### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.raw_web_events (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT,
    event_id STRING,
    event_type STRING
)
COMMENT = 'Raw web clickstream events ingested from website (JSON)';
```




## Table: raw_orders

### Source Information
| Attribute        | Value                           |
| ---------------- | ------------------------------- |
| Source System    | Order Management System         |
| Data Format      | CSV                             |
| Update Frequency | Daily                           |
| Load Method      | Bulk                            |

### Column Definitions

| Column Name  | Data Type     | Description               | Sample Value              |
| ------------ | ------------- | ------------------------- | ------------------------- |
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded      | 2024-01-15 02:00:00       |
| source_file  | STRING        | Source file name          | orders_20240115.csv       |
| raw_data     | VARIANT       | Raw row converted to JSON | {"order_id":"o-1001",...} |
| order_id     | STRING        | Order identifier          | o-1001                    |
| customer_id  | STRING        | Customer identifier       | c-789                     |



### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.raw_orders (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT,
    order_id STRING,
    customer_id, STRING
)
COMMENT = 'Raw order transactions ingested from CSV exports';
```




## Table: raw_products

### Source Information
| Attribute        | Value                   |
| ---------------- | ------------------------|
| Source System    | CRM                     |
| Data Format      | Database extract       |
| Update Frequency | Hourly                |
| Load Method      | Bulk |



### Column Definitions

| Column Name  | Data Type     | Description               | Sample Value               |
| ------------ | ------------- | ------------------------- | -------------------------- |
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded      | 2024-01-15 09:00:00        |
| source_file  | STRING        | API batch identifier      | products_api_20240115.json |
| raw_data     | VARIANT       | Full product JSON payload | {"product_id":"p-001",...} |
| product_id   | STRING        | Product identifier        | p-001                      |
| name         | STRING        | Product name              | Laptop Pro 15              |


### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.raw_products (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT,
    product_id STRING,
    name STRING
)
COMMENT = 'Raw product catalog data ingested from inventory API';
```




## Table: raw_customers

### Source Information
| Attribute        | Value                   |
| ---------------- | ------------------------|
| Source System    | CRM                     |
| Data Format      | Database extract       |
| Update Frequency | Daily                |
| Load Method      | Bulk |

### Column Definitions

| Column Name  | Data Type     | Description                 | Sample Value                            |
| ------------ | ------------- | --------------------------- | --------------------------------------- |
| ingestion_ts | TIMESTAMP_NTZ | When data was loaded        | 2024-01-15 03:00:00                     |
| source_file  | STRING        | Extract file name           | customers_20240115.csv                  |
| raw_data     | VARIANT       | Raw record as JSON          | {"customer_id":"c-789",...}             |
| customer_id  | STRING        | Customer identifier         | c-789                                   |
| email        | STRING        | Customer email              | [user@email.com](mailto:user@email.com) |


### DDL Statement

```sql
CREATE OR REPLACE TABLE BRONZE.raw_customers (
    ingestion_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    source_file STRING,
    raw_data VARIANT,
    customer_id STRING,
    email String
)
COMMENT = 'Raw customer profile data ingested from CRM system';
```