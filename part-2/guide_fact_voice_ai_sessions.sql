-- fact_voice_ai_sessions.sql
-- ═══════════════════════════════════════════════════════════════════
-- FACT TABLE: fact_voice_ai_sessions
-- Layer:      Mart (final, analytics-facing)
-- Grain:      One row per voice session
-- Owner:      Data Analytics Team — Irembo AI Project
-- Updated:    2025
-- 
-- HOW TO USE IN METABASE:
--   Browse Data → fact_voice_ai_sessions
--   All KPI fields are pre-computed — no joins needed.
--   Filter by is_vulnerable_user, region, session_month, etc.
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE fact_voice_ai_sessions AS

WITH base AS (
    SELECT * FROM int_sessions_enriched
)

SELECT
    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 1: IDENTIFIERS                   ║
    -- ╚══════════════════════════════════════════╝
    session_id,
    user_id,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 2: SESSION DIMENSIONS             ║
    -- ╚══════════════════════════════════════════╝
    channel,
    language,
    session_date,
    session_month,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 3: USER DIMENSIONS                ║
    -- ╚══════════════════════════════════════════╝
    region,
    is_disabled,
    is_first_time_user,
    is_vulnerable_user,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 4: SESSION OUTCOME METRICS        ║
    -- ╚══════════════════════════════════════════╝
    final_outcome,
    is_completed,
    is_abandoned,
    is_transferred,
    transfer_reason,
    total_duration_sec,
    total_turns,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 5: AI PERFORMANCE METRICS         ║
    -- ╚══════════════════════════════════════════╝
    avg_asr_confidence,
    avg_intent_confidence,
    misunderstanding_rate,
    silence_rate,
    is_recovered,
    is_escalated,
    had_errors,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 6: TURN-LEVEL AGGREGATES          ║
    -- ╚══════════════════════════════════════════╝
    user_turns,
    system_turns,
    total_error_turns,
    misunderstanding_turns,
    silence_turns,
    noise_turns,
    turn_error_rate_pct,
    repeat_intent_rate_pct,
    intent_service_lookup_count,
    intent_start_application_count,
    intent_repeat_count,
    intent_unknown_count,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 7: APPLICATION OUTCOMES           ║
    -- ╚══════════════════════════════════════════╝
    total_application_attempts,
    had_successful_application,
    applications_completed,
    applications_abandoned,
    applications_failed,
    services_attempted,
    primary_service_code,
    application_channel,
    avg_time_to_submit_sec,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 8: KPI FLAGS (PRE-COMPUTED)       ║
    -- ╚══════════════════════════════════════════╝

    -- Accessibility KPIs
    is_vulnerable_user                                  AS kpi_accessibility_segment,
    vulnerable_user_completed                           AS kpi_vulnerable_completed,
    (is_disabled AND is_completed)                      AS kpi_disabled_completed,
    (is_first_time_user AND is_completed)               AS kpi_firsttime_completed,
    (is_escalated AND is_vulnerable_user)               AS kpi_vulnerable_escalated,

    -- Efficiency KPIs
    end_to_end_success                                  AS kpi_end_to_end_success,
    (is_recovered AND had_errors)                       AS kpi_error_recovery_success,
    high_misunderstanding_session                       AS kpi_high_misunderstanding,
    high_silence_session                                AS kpi_high_silence,
    high_confusion_session                              AS kpi_high_confusion,

    -- Adoption KPIs
    had_application_attempt                             AS kpi_reached_application,
    (had_successful_application = 1)                    AS kpi_application_success,

    -- ╔══════════════════════════════════════════╗
    -- ║  BLOCK 9: SCORING (ADVANCED)             ║
    -- ╚══════════════════════════════════════════╝
    -- A composite AI quality score (0–100) for ranking sessions.
    -- Formula: penalise for misunderstanding, silence, escalation;
    --          reward for recovery, completion, low confusion.
    -- Useful for trend monitoring and anomaly detection.
    ROUND(LEAST(100, GREATEST(0,
        100
        - (COALESCE(misunderstanding_rate, 0) * 30)   -- up to -30 for 100% misunderstanding
        - (COALESCE(silence_rate, 0)          * 20)   -- up to -20 for 100% silence
        - (CASE WHEN is_escalated THEN 10 ELSE 0 END) -- -10 if escalated
        - (COALESCE(repeat_intent_rate_pct,0) * 0.1)  -- small penalty for high repeat %
        + (CASE WHEN is_recovered THEN 10 ELSE 0 END) -- +10 if recovered
        + (CASE WHEN is_completed THEN 10 ELSE 0 END) -- +10 if completed
    )), 1)                                              AS ai_quality_score

FROM base