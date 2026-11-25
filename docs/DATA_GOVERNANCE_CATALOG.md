# Marketing Data Governance & AI-Readiness Catalog

## 1. Grain & Join Fan-Out Rules
- `MART_ADVISOR_360` is strictly keyed at **1 row per deduplicated `ADVISOR_ID`**.
- All 1-to-many child tables (`MARKETING_TOUCHES`, `EVENT_ATTENDANCE`, `INQUIRY_ENRICHMENT`) **must** be pre-aggregated in dedicated CTEs (`STG_CAMPAIGN_PREAGG`, `STG_EVENT_PREAGG`) before joining onto `STG_ADVISOR_DEDUP`.
- Automated SQL integrity gate verifies `SUM(pipeline_influence_usd)` in `MART_ADVISOR_360` matches `SUM(pipeline_influence_usd)` in raw `EVENT_ATTENDANCE` (`$0.00` variance).

## 2. Deduplication & Timezone Standardization
- CRM records from HubSpot/Salesforce are deduplicated deterministically using `QUALIFY ROW_NUMBER() OVER (PARTITION BY EMAIL_HASH_SHA256 ORDER BY UPDATED_AT_UTC DESC) = 1`.
- All raw UTC timestamps are normalized via `CONVERT_TIMEZONE('UTC', 'America/New_York', EVENT_TS_UTC)::DATE` so evening webinar/email events attribute cleanly to Eastern Time business days.

## 3. AI-Readiness & LLM Enrichment Contract
- Every unstructured advisor inquiry classified by the n8n REST API enrichment flow must emit a validated JSON payload containing `intent_category`, `topic_bucket`, `urgency_tier`, `next_best_action`, and `sla_hours`.
