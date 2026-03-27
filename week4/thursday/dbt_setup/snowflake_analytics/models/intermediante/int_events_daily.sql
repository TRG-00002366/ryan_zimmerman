{{
    config(
        materialized='view'
    )
}}

/*
    Intermediate model: Daily event aggregates
    - Groups events by date and type
    - Calculates daily metrics
*/

WITH events AS (
    -- ref() creates a dependency on stg_events
    -- dbt will run stg_events BEFORE this model
    SELECT * FROM {{ ref('stg_events') }}
),

daily_aggregates AS (
    SELECT
        event_date,
        event_type,
        
        -- Counts
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users,
        
        -- Amounts (only for events with amounts)
        SUM(COALESCE(amount, 0)) AS total_amount,
        AVG(COALESCE(amount, 0)) AS avg_amount,
        
        -- Time boundaries
        MIN(event_timestamp) AS first_event_at,
        MAX(event_timestamp) AS last_event_at
        
    FROM events
    GROUP BY event_date, event_type
)

SELECT * FROM daily_aggregates