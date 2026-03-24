--TASK 3

SELECT 
CURRENT_WAREHOUSE() AS warehouse,
CURRENT_DATABASE() AS database,
CURRENT_SCHEMA() AS schema,
CURRENT_ROLE() AS role;

/*
OUTPUT
+------------+-------------+--------+--------------+
| WAREHOUSE  | DATABASE    | SCHEMA | ROLE         |
|------------+-------------+--------+--------------|
| COMPUTE_WH | RYAN_DEV_DB | BRONZE | ACCOUNTADMIN |
+------------+-------------+--------+--------------+
1 Row(s) produced. Time Elapsed: 0.189s
*/


USE DATABASE SNOWFLAKE_SAMPLE_DATA;
USE SCHEMA TPCH_SF1;

-- Customer distribution by market segment
SELECT 
    C_MKTSEGMENT,
    COUNT(*) AS customer_count,
    ROUND(AVG(C_ACCTBAL), 2) AS avg_balance
FROM CUSTOMER
GROUP BY C_MKTSEGMENT
ORDER BY customer_count DESC;

/*
OUTPUT
+----------------------------------+
| status                           |
|----------------------------------|
| Statement executed successfully. |
+----------------------------------+
1 Row(s) produced. Time Elapsed: 0.171s
+----------------------------------+
| status                           |
|----------------------------------|
| Statement executed successfully. |
+----------------------------------+
1 Row(s) produced. Time Elapsed: 0.369s
+--------------+----------------+-------------+
| C_MKTSEGMENT | CUSTOMER_COUNT | AVG_BALANCE |
|--------------+----------------+-------------|
| HOUSEHOLD    |          30189 |     4500.76 |
| BUILDING     |          30142 |     4508.28 |
| FURNITURE    |          29968 |     4480.08 |
| MACHINERY    |          29949 |     4488.93 |
| AUTOMOBILE   |          29752 |     4499.42 |
+--------------+----------------+-------------+
5 Row(s) produced. Time Elapsed: 1.866s
*/


-- Top 10 customers by order volume
SELECT 
    c.C_NAME AS customer_name,
    n.N_NAME AS nation,
    COUNT(o.O_ORDERKEY) AS order_count,
    SUM(o.O_TOTALPRICE) AS total_spent
FROM CUSTOMER c
JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY
JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY
GROUP BY c.C_NAME, n.N_NAME
ORDER BY total_spent DESC
LIMIT 10;

/*
OUTPUT
+--------------------+------------+-------------+-------------+
| CUSTOMER_NAME      | NATION     | ORDER_COUNT | TOTAL_SPENT |
|--------------------+------------+-------------+-------------|
| Customer#000143500 | IRAN       |          39 |  7012696.48 |
| Customer#000095257 | BRAZIL     |          36 |  6563511.23 |
| Customer#000087115 | KENYA      |          34 |  6457526.26 |
| Customer#000131113 | ETHIOPIA   |          37 |  6311428.86 |
| Customer#000103834 | IRAQ       |          31 |  6306524.23 |
| Customer#000134380 | ALGERIA    |          37 |  6291610.15 |
| Customer#000069682 | MOZAMBIQUE |          39 |  6287149.42 |
| Customer#000102022 | INDONESIA  |          41 |  6273788.41 |
| Customer#000098587 | CHINA      |          37 |  6265089.35 |
| Customer#000085102 | MOROCCO    |          34 |  6135483.63 |
+--------------------+------------+-------------+-------------+
10 Row(s) produced. Time Elapsed: 2.365s
*/



--TASK 4
!spool /tmp/output.csv
SELECT * FROM NATION;
!spool off
--saves output to a file untill turned off, very useful for storing output while debugging/devolping.




--TASK 5

SELECT
QUERY_TEXT,
EXECUTION_STATUS,
TOTAL_ELAPSED_TIME/1000 AS seconds,
BYTES_SCANNED/1000000 AS mb_scanned,
ROWS_PRODUCED
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TYPE = 'SELECT'
ORDER BY START_TIME DESC
LIMIT 10;
-- Besides this query Customer distribution by market segment, took the longest and scanned the most data
-- Since were using group by and joins, bucketing could reduce the execution time.

/*
output
+---------------------------------------------------------------------------------+------------------+--------------------+------------+---------------+
| QUERY_TEXT                                                                      | EXECUTION_STATUS |            SECONDS | MB_SCANNED | ROWS_PRODUCED |
|---------------------------------------------------------------------------------+------------------+--------------------+------------+---------------|
| SELECT                                                                          | RUNNING          | -1774359126.261000 |   0.000000 |             0 |
|     QUERY_TEXT,                                                                 |                  |                    |            |               |
|     EXECUTION_STATUS,                                                           |                  |                    |            |               |
|     TOTAL_ELAPSED_TIME/1000 AS seconds,                                         |                  |                    |            |               |
|     BYTES_SCANNED/1000000 AS mb_scanned,                                        |                  |                    |            |               |
|     ROWS_PRODUCED                                                               |                  |                    |            |               |
| FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())                                  |                  |                    |            |               |
| WHERE QUERY_TYPE = 'SELECT'                                                     |                  |                    |            |               |
| ORDER BY START_TIME DESC                                                        |                  |                    |            |               |
| LIMIT 10;                                                                       |                  |                    |            |               |
| SELECT                                                                          | SUCCESS          |           2.294000 |  53.028016 |            10 |
|     c.C_NAME AS customer_name,                                                  |                  |                    |            |               |
|     n.N_NAME AS nation,                                                         |                  |                    |            |               |
|     COUNT(o.O_ORDERKEY) AS order_count,                                         |                  |                    |            |               |
|     SUM(o.O_TOTALPRICE) AS total_spent                                          |                  |                    |            |               |
| FROM CUSTOMER c                                                                 |                  |                    |            |               |
| JOIN ORDERS o ON c.C_CUSTKEY = o.O_CUSTKEY                                      |                  |                    |            |               |
| JOIN NATION n ON c.C_NATIONKEY = n.N_NATIONKEY                                  |                  |                    |            |               |
| GROUP BY c.C_NAME, n.N_NAME                                                     |                  |                    |            |               |
| ORDER BY total_spent DESC                                                       |                  |                    |            |               |
| LIMIT 10;                                                                       |                  |                    |            |               |
| SELECT                                                                          | SUCCESS          |           1.800000 |  10.741184 |             5 |
|     C_MKTSEGMENT,                                                               |                  |                    |            |               |
|     COUNT(*) AS customer_count,                                                 |                  |                    |            |               |
|     ROUND(AVG(C_ACCTBAL), 2) AS avg_balance                                     |                  |                    |            |               |
| FROM CUSTOMER                                                                   |                  |                    |            |               |
| GROUP BY C_MKTSEGMENT                                                           |                  |                    |            |               |
| ORDER BY customer_count DESC;                                                   |                  |                    |            |               |
| SELECT                                                                          | SUCCESS          |           0.124000 |   0.000000 |             0 |
|     CURRENT_WAREHOUSE() AS warehouse,                                           |                  |                    |            |               |
|     CURRENT_DATABASE() AS database,                                             |                  |                    |            |               |
|     CURRENT_SCHEMA() AS schema,                                                 |                  |                    |            |               |
|     CURRENT_ROLE() AS role;                                                     |                  |                    |            |               |
| SELECT CURRENT_USER(), CURRENT_DATABASE(), CURRENT_SCHEMA();                    | SUCCESS          |           0.086000 |   0.000000 |             0 |
| select * from IDENTIFIER('"RYAN_DEV_DB"."BRONZE"."STREAMING_EVENTS"') limit 100 | SUCCESS          |           0.336000 |   0.000000 |             0 |
| select * from IDENTIFIER('"RYAN_DEV_DB"."BRONZE"."STREAMING_EVENTS"') limit 100 | SUCCESS          |           1.593000 |   0.000000 |             0 |
| SELECT system$GET_NPS_FEEDBACK_TIMESTAMP(), system$GET_NPS_DISMISS_TIMESTAMP(); | SUCCESS          |           0.075000 |   0.000000 |             0 |
|                                                                                 | SUCCESS          |           0.125000 |   0.000000 |             1 |
|                                                                                 | SUCCESS          |           0.774000 |   0.000000 |             1 |
+---------------------------------------------------------------------------------+------------------+--------------------+------------+---------------+
*/