-- Lock the baseline: compute and store this before any intervention
SELECT
    'Jan-Apr 2025' AS baseline_period,
    COUNT(*) AS total_sessions,
    SUM(total_turns) AS total_turns,
    SUM(total_error_turns) AS total_error_turns,
    ROUND(AVG(turn_error_rate_pct), 1) AS baseline_avg_turn_error_rate_pct,
    -- 40% reduction target
    ROUND(AVG(turn_error_rate_pct) * 0.60, 1) AS target_turn_error_rate_pct,
    -- Secondary baseline: recovery rate
    ROUND(
        SUM(CASE WHEN is_recovered = 1
                      AND (misunderstanding_rate > 0 OR silence_rate > 0)
                 THEN 1.0 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN misunderstanding_rate > 0
                               OR silence_rate > 0
                          THEN 1 ELSE 0 END), 0) * 100
    , 1) AS baseline_recovery_rate_pct
FROM upload_fact_voice_ai_sessions_20260221143032
WHERE session_date < '2025-05-01';  -- explicit pre-intervention cutoff

