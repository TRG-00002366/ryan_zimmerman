{{
    config(
        materialized='table'
    )
}}

-- Join staging with aggregates for a denormalized view
WITH events AS (
    SELECT DISTINCT event_type FROM {{ ref('stg_events') }}
),

metrics AS (
    SELECT 
        event_type,
        SUM(event_count) AS total_events,
        SUM(unique_users) AS total_unique_users,
        SUM(total_amount) AS grand_total_amount
    FROM {{ ref('int_events_daily') }}
    GROUP BY event_type
)

SELECT
    e.event_type,
    COALESCE(m.total_events, 0) AS total_events,
    COALESCE(m.total_unique_users, 0) AS total_unique_users,
    COALESCE(m.grand_total_amount, 0) AS grand_total_amount
FROM events e
LEFT JOIN metrics m ON e.event_type = m.event_type