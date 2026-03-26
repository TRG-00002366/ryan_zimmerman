ERRORS
The provided code had some errors, for example down stream tasks must be in the same schema, snowflake won't allow dependencies that don't share a schema.
Also the PARSE_JSON function is not allowed in the VALUES section of an insert statement. To fix this I used SELECT and UNION ALL instead.

As expected before the proccess events task is executed the stream has 3 rows and silver has none.
After the first run the three rows added after the stream was created are now in silver and the stream is empty.
I then insert one more into raw events and run the task again, stream is empty, 4 events are in the silver table.

Task history result
NAME	STATE	SCHEDULED_TIME	COMPLETED_TIME	ERROR_MESSAGE
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 07:03:06.312 -0700	2026-03-25 07:03:09.327 -0700	
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 07:03:03.408 -0700	2026-03-25 07:03:05.094 -0700	
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 06:59:51.271 -0700	2026-03-25 06:59:52.944 -0700	
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 06:59:48.253 -0700	2026-03-25 06:59:51.271 -0700	
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 06:59:15.985 -0700	2026-03-25 06:59:17.694 -0700	
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 06:59:14.055 -0700	2026-03-25 06:59:15.985 -0700	
PROCESS_EVENTS_TASK	SUCCEEDED	2026-03-25 06:57:41.348 -0700	2026-03-25 06:57:43.075 -0700	

Check task dependencies result
2026-03-25 07:02:59.000 -0700	PROCESS_EVENTS_TASK	RYAN_DEV_DB	SILVER	ACCOUNTADMIN		COMPUTE_WH	1 MINUTE	[]	suspended	INSERT INTO SILVER.PROCESSED_EVENTS (event_id, event_type, user_id, event_data, source_timestamp)
SELECT 
    event_id,
    UPPER(event_type) AS event_type,
    payload:user::STRING AS user_id,
    payload AS event_data,
    created_at AS source_timestamp
FROM BRONZE.RAW_EVENTS_STREAM
WHERE METADATA$ACTION = 'INSERT'	SYSTEM$STREAM_HAS_DATA('BRONZE.RAW_EVENTS_STREAM')

2026-03-25 07:03:12.000 -0700	AGGREGATE_EVENTS_TASK	RYAN_DEV_DB	SILVER	ACCOUNTADMIN		COMPUTE_WH		[
  "RYAN_DEV_DB.SILVER.PROCESS_EVENTS_TASK"
]	suspended	MERGE INTO GOLD.EVENT_METRICS tgt
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
    VALUES (src.metric_date, src.event_type, src.event_count, src.unique_users)	
    