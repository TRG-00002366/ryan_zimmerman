-- Set your context
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- CRITICAL: Set cost-saving auto-suspend
ALTER WAREHOUSE COMPUTE_WH SET AUTO_SUSPEND = 60;

-- Create your personal database
CREATE DATABASE IF NOT EXISTS RYAN_DEV_DB
    COMMENT = 'Personal development database';

-- Create schemas for the Medallion architecture
CREATE SCHEMA IF NOT EXISTS RYAN_DEV_DB.BRONZE;
CREATE SCHEMA IF NOT EXISTS RYAN_DEV_DB.SILVER;
CREATE SCHEMA IF NOT EXISTS RYAN_DEV_DB.GOLD;

-- Verify creation
SHOW SCHEMAS IN DATABASE RYAN_DEV_DB;






USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- List all tables
SHOW TABLES;

-- Count rows in a large table
SELECT COUNT(*) FROM ORDERS;

-- Basic aggregation
SELECT 
    O_ORDERSTATUS,
    COUNT(*) AS order_count   --1.1s execution time
FROM ORDERS
GROUP BY O_ORDERSTATUS;