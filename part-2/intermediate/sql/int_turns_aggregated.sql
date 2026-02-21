-- stg_turns_aggregated.sql
-- Purpose: Aggregate turn-level data to session level.
-- Input:  6,500 rows (one per turn)
-- Output: ~1,196 rows (one per session)

SELECT
    session_id,

    -- Volume metrics
    COUNT(*) AS total_turns,
    COUNT(DISTINCT turn_number) AS unique_turn_positions,

    -- Speaker split
    COUNT(CASE WHEN speaker = 'user'   THEN 1 END) AS user_turns,
    COUNT(CASE WHEN speaker = 'system' THEN 1 END) AS system_turns,

    -- Intent distribution (useful for understanding what users were trying to do)
    COUNT(CASE WHEN detected_intent = 'service_lookup'    THEN 1 END) AS intent_service_lookup_count,
    COUNT(CASE WHEN detected_intent = 'start_application' THEN 1 END) AS intent_start_application_count,
    COUNT(CASE WHEN detected_intent = 'repeat'            THEN 1 END) AS intent_repeat_count,
    COUNT(CASE WHEN detected_intent = 'unknown'           THEN 1 END) AS intent_unknown_count,

    -- Error breakdown
    COUNT(CASE WHEN error_type != '' AND error_type IS NOT NULL THEN 1 END) AS total_error_turns,
    COUNT(CASE WHEN error_type = 'misunderstanding' THEN 1 END) AS misunderstanding_turns,
    COUNT(CASE WHEN error_type = 'silence'          THEN 1 END) AS silence_turns,
    COUNT(CASE WHEN error_type = 'noise'            THEN 1 END) AS noise_turns,

    -- Error rates at turn level (per session)
    ROUND(
        COUNT(CASE WHEN error_type != '' AND error_type IS NOT NULL THEN 1 END)
        * 100.0 / NULLIF(COUNT(*), 0), 1
    ) AS turn_error_rate_pct,

    -- Confusion signal: proportion of turns that were repeat requests
    ROUND(
        COUNT(CASE WHEN detected_intent = 'repeat' THEN 1 END)
        * 100.0 / NULLIF(COUNT(*), 0), 1
    ) AS repeat_intent_rate_pct,

    -- Average confidence scores at session level
    ROUND(AVG(CAST(intent_confidence AS NUMERIC)), 4) AS avg_turn_intent_confidence,
    ROUND(AVG(CAST(asr_confidence    AS NUMERIC)), 4) AS avg_turn_asr_confidence,

    -- Duration
    SUM(CAST(turn_duration_sec AS INTEGER)) AS total_turn_duration_sec,
    ROUND(AVG(CAST(turn_duration_sec AS INTEGER)), 1) AS avg_turn_duration_sec

FROM upload_voice_turns_20260220015613
GROUP BY session_id