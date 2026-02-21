-- KPI: What % of sessions result in a completed application?
SELECT
    region,
    is_vulnerable_user,
    COUNT(*) AS total_sessions,
    SUM(CASE WHEN kpi_end_to_end_success THEN 1 ELSE 0 END) AS end_to_end_successes,
    ROUND(AVG(CASE WHEN kpi_end_to_end_success THEN 1.0 ELSE 0 END) * 100, 1) AS e2e_success_rate_pct
FROM 
    upload_fact_voice_ai_sessions_20260221143032
GROUP BY 
    region, 
    is_vulnerable_user
ORDER BY 
    is_vulnerable_user DESC, 
    region;
