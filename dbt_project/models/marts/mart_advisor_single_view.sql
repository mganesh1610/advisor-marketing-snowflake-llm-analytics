{{ config(materialized='table', unique_key='advisor_id') }}

WITH advisor_dedup AS (
    SELECT
        advisor_id,
        email_hash_sha256,
        firm_segment,
        channel_preference,
        updated_at_utc
    FROM {{ source('crm', 'crm_advisors') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY email_hash_sha256
        ORDER BY updated_at_utc DESC
    ) = 1
),

campaign_preagg AS (
    SELECT
        advisor_id,
        COUNT(DISTINCT campaign_touch_id) AS campaign_touches,
        SUM(converted_flag) AS campaign_conversions,
        MAX(CONVERT_TIMEZONE('UTC', 'America/New_York', event_ts_utc)::DATE) AS last_touch_date_est
    FROM {{ source('martech', 'marketing_touches') }}
    GROUP BY advisor_id
),

event_preagg AS (
    SELECT
        advisor_id,
        COUNT(DISTINCT event_id) AS events_attended,
        SUM(post_event_activity_score) AS post_event_activity_score,
        SUM(pipeline_influence_usd) AS pipeline_influence_usd
    FROM {{ source('events', 'event_attendance') }}
    GROUP BY advisor_id
)

SELECT
    a.advisor_id,
    a.firm_segment,
    a.channel_preference,
    COALESCE(c.campaign_touches, 0) AS campaign_touches,
    COALESCE(c.campaign_conversions, 0) AS campaign_conversions,
    c.last_touch_date_est,
    COALESCE(e.events_attended, 0) AS events_attended,
    COALESCE(e.post_event_activity_score, 0) AS post_event_activity_score,
    COALESCE(e.pipeline_influence_usd, 0.0) AS pipeline_influence_usd
FROM advisor_dedup a
LEFT JOIN campaign_preagg c ON a.advisor_id = c.advisor_id
LEFT JOIN event_preagg e ON a.advisor_id = e.advisor_id
