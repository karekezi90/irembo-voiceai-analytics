-- stg_ai_metrics.sql
-- Purpose: Clean AI performance metrics.
-- One row per session (pre-aggregated in source).

SELECT
    session_id,

    -- Confidence scores — already numeric in source but cast for safety
    CAST(avg_asr_confidence     AS NUMERIC(5,4)) AS avg_asr_confidence,
    CAST(avg_intent_confidence  AS NUMERIC(5,4)) AS avg_intent_confidence,

    -- Error rates — values between 0.0 and 1.0
    CAST(misunderstanding_rate  AS NUMERIC(5,4)) AS misunderstanding_rate,
    CAST(silence_rate           AS NUMERIC(5,4)) AS silence_rate,

    -- Boolean flags
    (recovery_success = 'yes') AS is_recovered,
    (escalation_flag  = 'yes') AS is_escalated,

    -- Derived: session had any error (misunderstanding OR silence)
    (CAST(misunderstanding_rate AS NUMERIC) > 0
     OR CAST(silence_rate AS NUMERIC) > 0)          AS had_errors

FROM upload_voice_ai_metrics_20260220015601
