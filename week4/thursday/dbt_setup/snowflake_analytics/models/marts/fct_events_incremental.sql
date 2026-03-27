{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='merge'
    )
}}

WITH new_events AS (
    SELECT
        event_id,
        event_type,
        user_id,
        page_url,
        product_id,
        amount,
        event_timestamp,
        _dbt_loaded_at
    FROM {{ ref('stg_events') }}
    
    {% if is_incremental() %}
    -- Only get new records since last run
    WHERE event_timestamp > (
        SELECT COALESCE(MAX(event_timestamp), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    *,
    CURRENT_TIMESTAMP() AS _processed_at
FROM new_events