{{
    config(
        materialized='table'
    )
}}

/*
    Mart model: Daily metrics fact table
    - Business-ready aggregates
    - Includes calculated KPIs
    - Materialized as table for BI tool performance
*/

WITH daily_events AS (
    SELECT * FROM {{ ref('int_events_daily') }}
),

enriched AS (
    SELECT
        -- Dimensions
        event_date,
        event_type,
        
        -- Measures
        event_count,
        unique_users,
        total_amount,
        
        -- Calculated KPIs
        ROUND(total_amount / NULLIF(event_count, 0), 2) AS revenue_per_event,
        ROUND(total_amount / NULLIF(unique_users, 0), 2) AS revenue_per_user,
        
        -- Time metadata
        first_event_at,
        last_event_at,
        DATEDIFF('minute', first_event_at, last_event_at) AS active_minutes,
        
        -- dbt metadata
        CURRENT_TIMESTAMP() AS _dbt_refreshed_at
        
    FROM daily_events
)

SELECT * FROM enriched
ORDER BY event_date DESC, event_type