-- Voice session outcomes and engagement depth
SELECT
    final_outcome,
    COUNT(*) AS sessions,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_all,
    ROUND(AVG(total_turns), 1) AS avg_turns,
    ROUND(AVG(total_duration_sec), 1) AS avg_duration_sec
FROM upload_fact_voice_ai_sessions_20260221143032
GROUP BY final_outcome
ORDER BY sessions DESC;



-- Channel completion and failure rates from applications.csv
SELECT
    channel,
    COUNT(*) AS total_applications,
    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed,
    SUM(CASE WHEN status = 'failed'    THEN 1 ELSE 0 END) AS failed,
    SUM(CASE WHEN status = 'abandoned' THEN 1 ELSE 0 END) AS abandoned,
    ROUND((SUM(CASE WHEN status = 'completed' THEN 1.0 ELSE 0 END) * 100) / COUNT(*) , 2) AS completion_rate_pct,
    ROUND((SUM(CASE WHEN status = 'failed' THEN 1.0 ELSE 0 END) * 100) / COUNT(*) , 2) AS failure_rate_pct,
    ROUND((SUM(CASE WHEN status = 'abandoned' THEN 1.0 ELSE 0 END) * 100) / COUNT(*) , 2) AS abandone_rate_pct
FROM upload_applications_20260220015647
GROUP BY channel
ORDER BY completion_rate_pct DESC;
