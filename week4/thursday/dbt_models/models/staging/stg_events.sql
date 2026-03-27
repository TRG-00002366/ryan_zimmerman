{{
    config(
        materialized='view'
    )
}}

/*
    Staging model for raw events.
    - Extracts fields from JSON payload
    - Normalizes event_type to uppercase
    - Filters out null event_ids
*/

WITH source_data AS (
    -- source() references tables defined in sources.yml
    SELECT * FROM {{ source('bronze', 'raw_events') }}
),

transformed AS (
    SELECT
        -- Primary key
        event_id,
        
        -- Normalized event type
        UPPER(event_type) AS event_type,
        
        -- Extracted payload fields (adjust based on your data!)
        -- If payload is VARIANT/JSON:
        payload:user::STRING AS user_id,
        payload:page::STRING AS page_url,
        payload:product::STRING AS product_id,
        payload:amount::DECIMAL(10,2) AS amount,
        
        -- Timestamp handling
        created_at AS event_timestamp,
        DATE(created_at) AS event_date,
        
        -- dbt metadata
        CURRENT_TIMESTAMP() AS _dbt_loaded_at
        
    FROM source_data
    WHERE event_id IS NOT NULL
)

SELECT * FROM transformed