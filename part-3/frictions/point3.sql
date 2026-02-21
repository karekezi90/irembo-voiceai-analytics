-- Compare what signals actually differ between escalated and non-escalated sessions
SELECT
    is_escalated,
    COUNT(*) AS sessions,
    ROUND(AVG(silence_rate) * 100, 1) AS avg_silence_rate_pct,
    ROUND(AVG(misunderstanding_rate) * 100, 1) AS avg_misunderstanding_rate_pct,
    ROUND(AVG(avg_asr_confidence),   3) AS avg_asr_confidence,
    ROUND(AVG(CAST(is_recovered AS FLOAT)) * 100, 1) AS recovery_rate_pct,
    ROUND(AVG(CAST(is_completed AS FLOAT)) * 100, 1) AS completion_rate_pct
FROM upload_fact_voice_ai_sessions_20260221143032
WHERE is_escalated IS NOT NULL
GROUP BY is_escalated
ORDER BY is_escalated DESC;


-- Proposed new escalation rule — flag sessions that SHOULD have been escalated
SELECT
    COUNT(*) AS should_have_escalated,
    SUM(CASE WHEN is_escalated = 1 THEN 1 ELSE 0 END) AS actually_escalated,
    SUM(CASE WHEN is_escalated = 0 THEN 1 ELSE 0 END) AS missed_escalations
FROM upload_fact_voice_ai_sessions_20260221143032
WHERE misunderstanding_rate > 0.30
  AND is_recovered = 0;

