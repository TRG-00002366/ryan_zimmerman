USE DATABASE RYAN_DEV_DB;
USE SCHEMA BRONZE;

-- Ensure RAW_EVENTS table exists and has data
CREATE OR REPLACE TABLE RAW_EVENTS (
    event_id STRING,
    event_type STRING,
    payload VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Insert initial data
INSERT INTO RAW_EVENTS (event_id, event_type, payload)
    SELECT 'E001', 'click', PARSE_JSON('{"page": "/home", "user": "U100"}')
    UNION ALL SELECT 'E002', 'view', PARSE_JSON('{"page": "/products", "user": "U101"}')
    UNION ALL SELECT 'E003', 'purchase', PARSE_JSON('{"product": "P001", "amount": 99.99, "user": "U100"}');

SELECT * FROM RAW_EVENTS;

-- Create a stream to track changes
CREATE OR REPLACE STREAM RAW_EVENTS_STREAM ON TABLE RAW_EVENTS
    APPEND_ONLY = FALSE
    COMMENT = 'Tracks all changes to RAW_EVENTS';

-- Stream is empty because we created it AFTER the inserts
SELECT * FROM RAW_EVENTS_STREAM;

-- Now insert new data
INSERT INTO RAW_EVENTS (event_id, event_type, payload)
    SELECT 'E004', 'click', PARSE_JSON('{"page": "/checkout", "user": "U102"}')
    UNION ALL SELECT 'E005', 'purchase', PARSE_JSON('{"product": "P002", "amount": 149.99, "user": "U101"}')
    UNION ALL SELECT 'E006', 'click', PARSE_JSON('{"page": "/checkout", "user": "U100"}');

-- Stream now shows the new rows
SELECT * FROM RAW_EVENTS_STREAM;



USE SCHEMA SILVER;

CREATE OR REPLACE TABLE PROCESSED_EVENTS (
    event_id STRING PRIMARY KEY,
    event_type STRING,
    user_id STRING,
    event_data VARIANT,
    source_timestamp TIMESTAMP_NTZ,
    processed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create a task that processes stream data
CREATE OR REPLACE TASK PROCESS_EVENTS_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('BRONZE.RAW_EVENTS_STREAM')
AS
INSERT INTO SILVER.PROCESSED_EVENTS (event_id, event_type, user_id, event_data, source_timestamp)
SELECT 
    event_id,
    UPPER(event_type) AS event_type,
    payload:user::STRING AS user_id,
    payload AS event_data,
    created_at AS source_timestamp
FROM BRONZE.RAW_EVENTS_STREAM
WHERE METADATA$ACTION = 'INSERT';

-- View task definition
SHOW TASKS;
DESCRIBE TASK PROCESS_EVENTS_TASK;


-- Before executing, check stream contents
SELECT 'Stream before:' AS status, COUNT(*) AS num_rows FROM BRONZE.RAW_EVENTS_STREAM;
SELECT 'Silver before:' AS status, COUNT(*) AS num_rows FROM SILVER.PROCESSED_EVENTS;
    
-- Execute task manually
EXECUTE TASK PROCESS_EVENTS_TASK;

-- Check results
SELECT 'Stream after:' AS status, COUNT(*) AS num_rows FROM BRONZE.RAW_EVENTS_STREAM;
SELECT 'Silver after:' AS status, COUNT(*) AS num_rows FROM SILVER.PROCESSED_EVENTS;

INSERT INTO BRONZE.RAW_EVENTS (event_id, event_type, payload)
    SELECT 'E007', 'purchase', PARSE_JSON('{"product": "P003", "amount": 139.99, "user": "U102"}');
    
EXECUTE TASK PROCESS_EVENTS_TASK;

-- Check results again
SELECT 'Stream after 2:' AS status, COUNT(*) AS num_rows FROM BRONZE.RAW_EVENTS_STREAM;
SELECT 'Silver after 2:' AS status, COUNT(*) AS num_rows FROM SILVER.PROCESSED_EVENTS;

-- View processed data
SELECT * FROM SILVER.PROCESSED_EVENTS;



USE SCHEMA GOLD;

-- Create aggregation table
CREATE OR REPLACE TABLE EVENT_METRICS (
    metric_date DATE,
    event_type STRING,
    event_count INTEGER,
    unique_users INTEGER,
    refreshed_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (metric_date, event_type)
);

USE SCHEMA SILVER;

-- Create downstream task (must be in same schema as predecessor)
CREATE OR REPLACE TASK AGGREGATE_EVENTS_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER RYAN_DEV_DB.SILVER.PROCESS_EVENTS_TASK
AS
MERGE INTO GOLD.EVENT_METRICS tgt
USING (
    SELECT 
        DATE_TRUNC('day', processed_at)::DATE AS metric_date,
        event_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM SILVER.PROCESSED_EVENTS
    GROUP BY 1, 2
) src
ON tgt.metric_date = src.metric_date AND tgt.event_type = src.event_type
WHEN MATCHED THEN UPDATE SET 
    event_count = src.event_count,
    unique_users = src.unique_users,
    refreshed_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT 
    (metric_date, event_type, event_count, unique_users)
    VALUES (src.metric_date, src.event_type, src.event_count, src.unique_users);

-- View task history
SELECT 
    NAME,
    STATE,
    SCHEDULED_TIME,
    COMPLETED_TIME,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    TASK_NAME => 'PROCESS_EVENTS_TASK',
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
));

-- Check task dependencies
SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_DEPENDENTS(
    TASK_NAME => 'PROCESS_EVENTS_TASK',
    RECURSIVE => TRUE
));