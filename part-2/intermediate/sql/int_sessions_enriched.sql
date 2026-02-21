-- int_sessions_enriched.sql
-- Purpose: Join all staged sources into one enriched session-level table.
-- This is the core business logic layer.
-- Input:  stg_sessions, stg_users, stg_ai_metrics, stg_turns_aggregated, stg_applications_per_session
-- Output: one row per session, fully enriched

WITH stg_users AS (
    SELECT
        user_id,
        LOWER(region) AS region,
        (disability_flag = 'yes') AS is_disabled,
        (first_time_digital_user = 'yes') AS is_first_time_user,
        (disability_flag = 'yes' OR first_time_digital_user = 'yes') AS is_vulnerable_user
    FROM upload_users_20260220015436
),
stg_sessions AS (
    SELECT
        session_id,
        user_id,
        channel,
        language,
        CAST(total_duration_sec AS INTEGER) AS total_duration_sec,
        CAST(total_turns        AS INTEGER) AS total_turns,
        final_outcome,
        (final_outcome = 'completed') AS is_completed,
        (final_outcome = 'abandoned') AS is_abandoned,
        (final_outcome = 'transferred') AS is_transferred,
        NULLIF(TRIM(transfer_reason), '') AS transfer_reason,
        CAST(created_at AS DATE)            AS session_date,
        DATE_TRUNC('month', CAST(created_at AS DATE))   AS session_month
    FROM upload_voice_sessions_20260220015550
),
stg_ai_metrics AS (
    SELECT
        session_id,
        CAST(avg_asr_confidence     AS NUMERIC(5,4)) AS avg_asr_confidence,
        CAST(avg_intent_confidence  AS NUMERIC(5,4)) AS avg_intent_confidence,
        CAST(misunderstanding_rate  AS NUMERIC(5,4)) AS misunderstanding_rate,
        CAST(silence_rate           AS NUMERIC(5,4)) AS silence_rate,
        (recovery_success = 'yes') AS is_recovered,
        (escalation_flag  = 'yes') AS is_escalated,
        (CAST(misunderstanding_rate AS NUMERIC) > 0 OR CAST(silence_rate AS NUMERIC) > 0) AS had_errors
    FROM upload_voice_ai_metrics_20260220015601
), 
int_turns_aggregated AS (
    SELECT
        session_id,
        COUNT(*) AS total_turns,
        COUNT(DISTINCT turn_number) AS unique_turn_positions,
        COUNT(CASE WHEN speaker = 'user'   THEN 1 END) AS user_turns,
        COUNT(CASE WHEN speaker = 'system' THEN 1 END) AS system_turns,
        COUNT(CASE WHEN detected_intent = 'service_lookup'    THEN 1 END) AS intent_service_lookup_count,
        COUNT(CASE WHEN detected_intent = 'start_application' THEN 1 END) AS intent_start_application_count,
        COUNT(CASE WHEN detected_intent = 'repeat'            THEN 1 END) AS intent_repeat_count,
        COUNT(CASE WHEN detected_intent = 'unknown'           THEN 1 END) AS intent_unknown_count,
        COUNT(CASE WHEN error_type != '' AND error_type IS NOT NULL THEN 1 END) AS total_error_turns,
        COUNT(CASE WHEN error_type = 'misunderstanding' THEN 1 END) AS misunderstanding_turns,
        COUNT(CASE WHEN error_type = 'silence'          THEN 1 END) AS silence_turns,
        COUNT(CASE WHEN error_type = 'noise'            THEN 1 END) AS noise_turns,
        ROUND(
            COUNT(CASE WHEN error_type != '' AND error_type IS NOT NULL THEN 1 END)
            * 100.0 / NULLIF(COUNT(*), 0), 1
        ) AS turn_error_rate_pct,
        ROUND(
            COUNT(CASE WHEN detected_intent = 'repeat' THEN 1 END)
            * 100.0 / NULLIF(COUNT(*), 0), 1
        ) AS repeat_intent_rate_pct,
        ROUND(AVG(CAST(intent_confidence AS NUMERIC)), 4) AS avg_turn_intent_confidence,
        ROUND(AVG(CAST(asr_confidence    AS NUMERIC)), 4) AS avg_turn_asr_confidence,
        SUM(CAST(turn_duration_sec AS INTEGER)) AS total_turn_duration_sec,
        ROUND(AVG(CAST(turn_duration_sec AS INTEGER)), 1) AS avg_turn_duration_sec
    FROM upload_voice_turns_20260220015613
    GROUP BY session_id
),
int_applications_per_session AS (
    SELECT
        session_id,
        COUNT(*) AS total_application_attempts,
        MAX(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS had_successful_application,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) AS applications_completed,
        COUNT(CASE WHEN status = 'abandoned' THEN 1 END) AS applications_abandoned,
        COUNT(CASE WHEN status = 'failed'    THEN 1 END) AS applications_failed,
        groupArray(DISTINCT service_code) AS services_attempted,
        arrayElement(topK(1)(service_code), 1) AS primary_service_code,
        arrayElement(topK(1)(channel), 1) AS application_channel,
        MIN(CASE WHEN status='completed' THEN time_to_submit_sec END) AS min_time_to_submit_sec,
        AVG(CASE WHEN status='completed' THEN time_to_submit_sec END) AS avg_time_to_submit_sec
    FROM upload_applications_20260220015647
    GROUP BY session_id
),
int_sessions_enriched AS (
    SELECT
        -- ── IDENTIFIERS ─────────────────────────────────────────
        s.session_id,
        s.user_id,

        -- ── SESSION ATTRIBUTES ──────────────────────────────────
        s.channel,
        s.language,
        s.total_duration_sec,
        s.total_turns,
        s.final_outcome,
        s.is_completed,
        s.is_abandoned,
        s.is_transferred,
        s.transfer_reason,
        s.session_date,
        s.session_month,

        -- ── USER ATTRIBUTES ─────────────────────────────────────
        u.region,
        u.is_disabled,
        u.is_first_time_user,
        u.is_vulnerable_user,

        -- ── AI PERFORMANCE METRICS ──────────────────────────────
        -- LEFT JOIN: some sessions may not have metrics yet
        m.avg_asr_confidence,
        m.avg_intent_confidence,
        m.misunderstanding_rate,
        m.silence_rate,
        m.is_recovered,
        m.is_escalated,
        m.had_errors,

        -- ── TURN-LEVEL AGGREGATES ────────────────────────────────
        t.user_turns,
        t.system_turns,
        t.total_error_turns,
        t.misunderstanding_turns,
        t.silence_turns,
        t.noise_turns,
        t.turn_error_rate_pct,
        t.repeat_intent_rate_pct,
        t.intent_service_lookup_count,
        t.intent_start_application_count,
        t.intent_repeat_count,
        t.intent_unknown_count,
        t.avg_turn_intent_confidence,
        t.avg_turn_asr_confidence,

        -- ── APPLICATION OUTCOMES ─────────────────────────────────
        -- LEFT JOIN: many sessions have no application (browsing/info sessions)
        COALESCE(a.total_application_attempts, 0)   AS total_application_attempts,
        COALESCE(a.had_successful_application, 0)   AS had_successful_application,
        COALESCE(a.applications_completed, 0)       AS applications_completed,
        COALESCE(a.applications_abandoned, 0)       AS applications_abandoned,
        COALESCE(a.applications_failed, 0)          AS applications_failed,
        a.services_attempted,
        a.primary_service_code,
        a.application_channel,
        a.avg_time_to_submit_sec,

        -- ── DERIVED FLAGS FOR KPI REPORTING ─────────────────────
        -- These are pre-computed flags that make Metabase queries trivial

        -- Was this a session with high confusion? (repeat > 20% of turns)
        (COALESCE(t.repeat_intent_rate_pct, 0) > 20)        AS high_confusion_session,

        -- Was this session AI-quality problematic?
        (COALESCE(m.misunderstanding_rate, 0) > 0.3)        AS high_misunderstanding_session,
        (COALESCE(m.silence_rate, 0) > 0.3)                 AS high_silence_session,

        -- Did this session result in an application attempt?
        (COALESCE(a.total_application_attempts, 0) > 0)     AS had_application_attempt,

        -- End-to-end success: session completed AND at least one application succeeded
        (s.is_completed AND COALESCE(a.had_successful_application, 0) = 1)
                                                            AS end_to_end_success,

        -- Accessibility parity flag: vulnerable user who completed
        (u.is_vulnerable_user AND s.is_completed)            AS vulnerable_user_completed

    FROM stg_sessions s
    LEFT JOIN stg_users         u  ON s.user_id    = u.user_id
    LEFT JOIN stg_ai_metrics    m  ON s.session_id = m.session_id
    LEFT JOIN int_turns_aggregated     t  ON s.session_id = t.session_id
    LEFT JOIN int_applications_per_session      a  ON s.session_id = a.session_id
)

SELECT * FROM int_sessions_enriched