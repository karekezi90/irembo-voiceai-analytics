-- KPI - Efficiency metrics over time
SELECT
    session_month,
    COUNT(*) AS total_sessions,
    ROUND(AVG(CASE WHEN is_completed  THEN 1.0 ELSE 0 END) * 100, 1) AS completion_rate_pct,
    ROUND(AVG(CASE WHEN is_recovered AND had_errors THEN 1.0 ELSE 0 END) * 100, 1) AS recovery_rate_pct,
    ROUND(AVG(turn_error_rate_pct), 1) AS avg_turn_error_rate_pct,
    ROUND(AVG(ai_quality_score), 1) AS avg_ai_quality_score
FROM 
    upload_fact_voice_ai_sessions_20260221143032
GROUP BY 
    session_month
ORDER BY 
    session_month;
