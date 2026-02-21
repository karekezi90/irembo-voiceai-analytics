-- Recovery rate among sessions that had errors
SELECT
    COUNT(*) AS error_sessions,
    SUM(CASE WHEN is_recovered = 1 THEN 1 ELSE 0 END) AS recovered,
    SUM(CASE WHEN is_recovered = 0 THEN 1 ELSE 0 END) AS not_recovered,
    ROUND(AVG(CAST(is_recovered AS FLOAT)) * 100, 1) AS recovery_rate_pct
FROM upload_fact_voice_ai_sessions_20260221143032
WHERE had_errors = 1;

-- Late vs early abandonment
SELECT
    CASE WHEN total_turns > 8
         THEN 'Late abandonment (>8 turns)'
         ELSE 'Early abandonment (<=8 turns)' END AS abandonment_type,
    COUNT(*) AS sessions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_all_abandoned
FROM upload_fact_voice_ai_sessions_20260221143032
WHERE is_abandoned = 1
GROUP BY 1
ORDER BY 2 DESC;