-- KPI - Completion rates by user segment
-- One query powers three Metabase charts
SELECT
    region,
    is_disabled,
    is_first_time_user,
    is_vulnerable_user,
    COUNT(*) AS total_sessions,
    SUM(CASE WHEN is_completed THEN 1 ELSE 0 END)  AS completed,
    ROUND(AVG(CASE WHEN is_completed THEN 1.0 ELSE 0 END) * 100, 1) AS completion_rate_pct,
    ROUND(AVG(CASE WHEN is_escalated THEN 1.0 ELSE 0 END) * 100, 1) AS escalation_rate_pct
FROM upload_fact_voice_ai_sessions_20260221143032
GROUP BY region, is_disabled, is_first_time_user, is_vulnerable_user
ORDER BY is_vulnerable_user DESC, region;
