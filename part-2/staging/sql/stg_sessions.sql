-- stg_sessions.sql
-- Purpose: Clean and enrich voice_sessions source table.
-- One row per session.

SELECT
    session_id,
    user_id,
    channel,
    language,

    -- Numeric fields — cast explicitly (some SQL engines import CSVs as TEXT)
    CAST(total_duration_sec AS INTEGER) AS total_duration_sec,
    CAST(total_turns        AS INTEGER) AS total_turns,

    final_outcome,

    -- Boolean outcome flags — make filtering simpler downstream
    (final_outcome = 'completed') AS is_completed,
    (final_outcome = 'abandoned') AS is_abandoned,
    (final_outcome = 'transferred') AS is_transferred,

    -- Null-safe transfer reason
    NULLIF(TRIM(transfer_reason), '') AS transfer_reason,

    -- Date fields for time-series reporting
    CAST(created_at AS DATE)            AS session_date,
    DATE_TRUNC('month', CAST(created_at AS DATE))   AS session_month

FROM upload_voice_sessions_20260220015550