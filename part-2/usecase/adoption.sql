-- KPI: Monthly Active Users
SELECT
    session_month,
    COUNT(DISTINCT user_id) AS monthly_active_users,
    COUNT(DISTINCT CASE WHEN is_vulnerable_user THEN user_id END) AS vulnerable_mau,
    COUNT(*) AS total_sessions,
    SUM(CASE WHEN kpi_reached_application THEN 1 ELSE 0 END) AS sessions_with_applications,
    SUM(CASE WHEN kpi_application_success THEN 1 ELSE 0 END) AS successful_applications
FROM 
    upload_fact_voice_ai_sessions_20260221143032
GROUP BY 
    session_month
ORDER BY 
    session_month;
